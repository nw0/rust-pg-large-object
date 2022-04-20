//! An interface for PostgreSQL large objects.
//!
//! This crate uses `tokio-postgres` as its SQL driver.
#![warn(missing_docs)]
use std::cmp;
use std::future::Future;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use pin_project::pin_project;
use postgres_types::Oid;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use tokio_postgres::{Error, Row, Transaction};

/// Default chunk size for reading/writing large object.
///
/// 4M works well on my machine, not sure about yours.
const DEFAULT_OP_SZ: usize = 4_194_304;

const SEEK_SET: i32 = 0;
const SEEK_CUR: i32 = 1;
const SEEK_END: i32 = 2;

async fn loread(client: &Transaction<'_>, fd: i32, size: usize) -> Result<Row, Error> {
    let size = cmp::min(size, DEFAULT_OP_SZ) as i32;
    client
        .query_one("SELECT pg_catalog.loread($1, $2)", &[&fd, &size])
        .await
}

async fn lowrite(client: &Transaction<'_>, fd: i32, buf: Vec<u8>) -> Result<usize, Error> {
    let size = cmp::min(buf.len(), DEFAULT_OP_SZ) as i32;
    client
        .query_one("SELECT pg_catalog.lowrite($1, $2)", &[&fd, &size])
        .await
        .map(|row| row.get::<_, i32>(0) as usize)
}

async fn lseek64(client: &Transaction<'_>, fd: i32, whence: SeekFrom) -> Result<u64, Error> {
    let (offset, whence) = match whence {
        SeekFrom::Start(n) => (n as i64, SEEK_SET),
        SeekFrom::Current(n) => (n, SEEK_CUR),
        SeekFrom::End(n) => (n, SEEK_END),
    };
    client
        .query_one(
            "SELECT pg_catalog.lo_lseek64($1, $2, $3)",
            &[&fd, &offset, &whence],
        )
        .await
        .map(|row| row.get::<_, i64>(0) as u64)
}

#[pin_project(project = LoStateEn, project_replace=LoStateOwned)]
enum LoState<'pin> {
    Seek(Pin<Box<dyn Future<Output = Result<u64, Error>> + 'pin>>),
    Read(Pin<Box<dyn Future<Output = Result<Row, tokio_postgres::Error>> + 'pin>>),
    Write(Pin<Box<dyn Future<Output = Result<usize, Error>> + 'pin>>),
    Wait,
}

/// A handle to a large object.
///
/// Large objects require [Transaction]s as a Postgres large object descriptor
/// is only valid for the duration of a transaction.
#[pin_project]
pub struct LargeObject<'a> {
    client: &'a Transaction<'a>,
    /// The PostgreSQL `Oid` of the large object.
    pub oid: Oid,
    fd: i32,
    #[pin]
    state: LoState<'a>,
}

impl<'a> LargeObject<'a> {
    /// Create a new large object.
    pub async fn new(client: &'a Transaction<'a>, mode: i32) -> Result<LargeObject<'a>, Error> {
        let row = client
            .query_one("SELECT pg_catalog.lo_creat(-1)", &[])
            .await?;
        let oid = row.get(0);
        Self::open(client, oid, mode).await
    }

    /// Open a large object.
    pub async fn open(
        client: &'a Transaction<'a>,
        oid: Oid,
        mode: i32,
    ) -> Result<LargeObject<'a>, Error> {
        let row = client
            .query_one("SELECT pg_catalog.lo_open($1, $2)", &[&(oid as i32), &mode])
            .await?;
        let fd = row.get(0);
        Ok(LargeObject {
            client,
            oid,
            fd,
            state: LoState::Wait,
        })
    }
}

impl<'a> AsyncSeek for LargeObject<'a> {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let mut this = self.project();
        let state = this.state.as_mut().project();
        match state {
            LoStateEn::Wait => {
                let res = lseek64(*this.client, *this.fd, position);
                this.state.project_replace(LoState::Seek(Box::pin(res)));
                Ok(())
            }
            _ => todo!(),
        }
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let mut this = self.project();
        let state = this.state.as_mut().project();
        match state {
            LoStateEn::Seek(fut) => {
                let offset = ready!(fut.as_mut().poll(cx));
                this.state.project_replace(LoState::Wait);
                Poll::Ready(offset.map_err(|_| todo!()))
            }
            _ => todo!(),
        }
    }
}

impl<'a> AsyncRead for LargeObject<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        let state = this.state.as_mut().project();
        match state {
            LoStateEn::Read(fut) => {
                let result = ready!(fut.as_mut().poll(cx));
                this.state.project_replace(LoState::Wait);
                let row = match result {
                    Ok(r) => r,
                    Err(e) => {
                        return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                    }
                };

                let chunk: Vec<u8> = row.get(0);
                match chunk.len() {
                    0 => Poll::Ready(Ok(())),
                    n => {
                        let at_most = std::cmp::min(buf.remaining(), n);
                        buf.put_slice(&chunk[..at_most]);
                        Poll::Ready(Ok(()))
                    }
                }
            }
            LoStateEn::Wait => {
                let row = loread(*this.client, *this.fd, buf.remaining());
                this.state.project_replace(LoState::Read(Box::pin(row)));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => todo!(),
        }
    }
}

impl<'a> AsyncWrite for LargeObject<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();
        let state = this.state.as_mut().project();
        match state {
            LoStateEn::Wait => {
                let buf: Vec<u8> = buf.into();
                let op = lowrite(*this.client, *this.fd, buf);
                this.state.project_replace(LoState::Write(Box::pin(op)));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            LoStateEn::Write(fut) => {
                // Do we assume that this buf is the same as the last one?
                let result = ready!(fut.as_mut().poll(cx));
                this.state.project_replace(LoState::Wait);
                Poll::Ready(result.map_err(|_| todo!()))
            }
            _ => todo!(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // No need to flush
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
