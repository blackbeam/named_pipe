//! Named-Pipe is a wrapper for overlapped (asyncronous) IO of Windows's named pipes.
//!
//! # Description
//!
//! You can use `wait` or `wait_all` to *select(2)*-like wait for multiple pending IO operations
//! (which is read/write from/to `PipeServer`/`PipeClient` or waiting for new client).
//!
//! Or you can use `ConnectingServer::wait` or `io::Read` and `io::Write` implementaions for
//! `PipeServer` and `PipeClient` for syncronous communication.
//!
//! For better understanding please refer to [Named Pipes documentation on MSDN]
//! (https://www.google.com/search?q=msdn+named+pipes&ie=utf-8&oe=utf-8).
//!
//! # Usage
//!
//! To create new pipe instance use [`PipeOptions`](struct.PipeOptions.html) structure.
//!
//! To connect to a pipe server use [`PipeClient`](struct.PipeClient.html) structure.
extern crate winapi;
extern crate kernel32;

use std::io;
use std::mem;
use std::ptr;
use std::sync::Arc;
use std::marker::PhantomData;
use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;

use kernel32::*;

use winapi::*;

struct Handle {
    value: HANDLE,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = unsafe { CloseHandle(self.value) };
    }
}

unsafe impl Sync for Handle { }
unsafe impl Send for Handle { }

struct Event {
    handle: Handle,
}

impl Event {
    fn new() -> io::Result<Event> {
        let handle = unsafe {
            CreateEventW(ptr::null_mut(),
                         1,
                         0,
                         ptr::null())
        };
        if handle != ptr::null_mut() {
            Ok(Event { handle: Handle { value: handle } })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn set(&self) -> io::Result<()> {
        let result = unsafe { SetEvent(self.handle.value) };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

struct Overlapped {
    ovl: OVERLAPPED,
    event: Event,
}

impl Overlapped {
    fn new() -> io::Result<Overlapped> {
        let event = try!(Event::new());
        let mut ovl: OVERLAPPED = unsafe { mem::zeroed() };
        ovl.hEvent = event.handle.value;
        Ok(Overlapped {
            ovl: ovl,
            event: event,
        })
    }

    fn clear(&mut self) {
        self.ovl = unsafe { mem::zeroed() };
        self.ovl.hEvent = self.event.handle.value;
    }

    fn get_mut(&mut self) -> &mut OVERLAPPED {
        &mut self.ovl
    }
}

pub enum OpenMode {
    /// Read only pipe instance
    Read,
    /// Write only pipe instance
    Write,
    /// Read-write pipe instance
    Duplex,
}

impl OpenMode {
    fn val(&self) -> u32 {
        match self {
            &OpenMode::Read => PIPE_ACCESS_INBOUND,
            &OpenMode::Write => PIPE_ACCESS_OUTBOUND,
            &OpenMode::Duplex => PIPE_ACCESS_DUPLEX,
        }
    }
}

/// Options and flags which can be used to configure how a pipe is created.
///
/// This builder exposes the ability to configure how a `ConnectingServer` is created.
///
/// Builder defaults:
///
/// - **open_mode** - `Duplex`
/// - **in_buffer** - 65536
/// - **out_buffer** - 65536
/// - **first** - true
pub struct PipeOptions {
    name: Arc<Vec<u16>>,
    open_mode: OpenMode,
    out_buffer: u32,
    in_buffer: u32,
    first: bool,
}

impl PipeOptions {
    fn create_named_pipe(&self, first: bool) -> io::Result<Handle> {
        let handle = unsafe {
            CreateNamedPipeW(self.name.as_ptr(),
                             (self.open_mode.val() | FILE_FLAG_OVERLAPPED |
                              if first {FILE_FLAG_FIRST_PIPE_INSTANCE} else {0}),
                             PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                             PIPE_UNLIMITED_INSTANCES,
                             65536,
                             65536,
                             0,
                             ptr::null_mut())
        };

        if handle != INVALID_HANDLE_VALUE {
            Ok(Handle { value: handle })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn new<T: AsRef<OsStr>>(name: T) -> PipeOptions {
        let mut full_name: OsString = name.as_ref().into();
        full_name.push("\x00");
        let full_name = full_name.encode_wide().collect::<Vec<u16>>();
        PipeOptions {
            name: Arc::new(full_name),
            open_mode: OpenMode::Duplex,
            out_buffer: 65536,
            in_buffer: 65536,
            first: true,
        }
    }

    /// Is this instance (or instances) will be first for this pipe name? Defaults to `true`.
    pub fn first(&mut self, val: bool) -> &mut PipeOptions {
        self.first = val;
        self
    }

    /// Open mode for pipe instance. Defaults to `Duplex`.
    pub fn open_mode(&mut self, val: OpenMode) -> &mut PipeOptions {
        self.open_mode = val;
        self
    }

    /// Input buffer size for pipe instance. Defaults to 65536
    pub fn in_buffer(&mut self, val: u32) -> &mut PipeOptions {
        self.in_buffer = val;
        self
    }

    /// Output buffer size for pipe instance. Defaults to 65536.
    pub fn out_buffer(&mut self, val: u32) -> &mut PipeOptions {
        self.out_buffer = val;
        self
    }

    /// Creates single instance of pipe with this options.
    pub fn single(&self) -> io::Result<ConnectingServer> {
        let mut pipes = try!(self.multiple(1));
        match pipes.pop() {
            Some(pipe) => Ok(pipe),
            None => unreachable!(),
        }
    }

    /// Creates multiple instances of pipe with this options.
    pub fn multiple(&self, num: u32) -> io::Result<Vec<ConnectingServer>> {
        if num == 0 {
            return Ok(Vec::new())
        }
        let mut out = Vec::with_capacity(num as usize);
        let mut first = self.first;
        for _ in 0..num {
            let handle = try!(self.create_named_pipe(first));
            first = false;
            let mut ovl = try!(Overlapped::new());
            let pending = try!(connect_named_pipe(&handle, &mut ovl));
            out.push(ConnectingServer {
                handle: handle,
                ovl: ovl,
                pending: pending,
            });
        }
        Ok(out)
    }
}

/// Pipe instance waiting for new client. Can be used with [`wait`](fn.wait.html) and [`wait_all`]
/// (fn.wait_all.html) functions.
pub struct ConnectingServer {
    handle: Handle,
    ovl: Overlapped,
    pending: bool,
}

impl ConnectingServer {
    /// Waites for client infinitely.
    pub fn wait(self) -> io::Result<PipeServer> {
        match try!(self.wait_ms(INFINITE)) {
            Ok(pipe_server) => Ok(pipe_server),
            Err(_) => unreachable!(),
        }
    }

    /// Waites for client. Note that `timeout` 0xFFFFFFFF stands for infinite waiting.
    pub fn wait_ms(mut self, timeout: u32) -> io::Result<Result<PipeServer, ConnectingServer>> {
        if self.pending {
            match try!(wait_for_single_obj(&mut self, timeout)) {
                Some(_) => { try!(get_ovl_result(&mut self)); },
                None => return Ok(Err(self)),
            }
        }
        let ConnectingServer { handle, mut ovl, ..} = self;
        ovl.clear();
        Ok(Ok(PipeServer {
            handle: Some(handle),
            ovl: Some(ovl),
        }))
    }
}

/// Pipe server connected to a client.
pub struct PipeServer {
    handle: Option<Handle>,
    ovl: Option<Overlapped>,
}

impl PipeServer {
    /// This function will flush buffers and disconnect server from client. Then will start waiting
    /// for a new client.
    pub fn unwrap(mut self) -> io::Result<ConnectingServer> {
        let handle = self.handle.take().unwrap();
        let mut ovl = self.ovl.take().unwrap();
        let mut result = unsafe { FlushFileBuffers(handle.value) };

        if result != 0 {
            result = unsafe { DisconnectNamedPipe(handle.value) };
            if result != 0 {
                ovl.clear();
                let pending = try!(connect_named_pipe(&handle, &mut ovl));
                Ok(ConnectingServer {
                    handle: handle,
                    ovl: ovl,
                    pending: pending,
                })
            } else {
                Err(io::Error::last_os_error())
            }
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Initializes asyncronous read opeation.
    pub fn read_async<'a, 'b: 'a>(&'a mut self, buf: &'b mut [u8]) -> io::Result<ReadHandle<'a, Self>> {
        init_read(self, buf)
    }

    /// Initializes asyncronous read operation and takes ownership of buffer and server.
    pub fn read_async_owned(self, buf: Vec<u8>) -> io::Result<ReadHandle<'static, Self>> {
        init_read_owned(self, buf)
    }

    /// Initializes asyncronous write operation.
    pub fn write_async<'a, 'b: 'a>(&'a mut self, buf: &'b [u8]) -> io::Result<WriteHandle<'a, Self>> {
        init_write(self, buf)
    }

    /// Initializes asyncronous write operation and takes ownership of buffer and server.
    pub fn write_async_owned(self, buf: Vec<u8>) -> io::Result<WriteHandle<'static, Self>> {
        init_write_owned(self, buf)
    }
}

impl io::Read for PipeServer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_async(buf).and_then(|read_handle| read_handle.wait()).map(|x| x.0)
    }
}

impl io::Write for PipeServer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_async(buf).and_then(|write_handle| write_handle.wait()).map(|x| x.0)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.handle {
            Some(ref handle) => {
                let result = unsafe { FlushFileBuffers(handle.value) };
                if result != 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            },
            None => unreachable!(),
        }
    }
}

impl Drop for PipeServer {
    fn drop(&mut self) {
        if let Some(ref handle) = self.handle {
            let _ = unsafe { FlushFileBuffers(handle.value) };
            let _ = unsafe { DisconnectNamedPipe(handle.value) };
        }
    }
}

/// Pipe client connected to a server.
pub struct PipeClient {
    handle: Handle,
    ovl: Overlapped,
}

impl PipeClient {
    fn create_file(name: &Vec<u16>) -> io::Result<Handle> {
        let mut handle = unsafe {
            CreateFileW(name.as_ptr(),
                        GENERIC_READ | GENERIC_WRITE,
                        0,
                        ptr::null_mut(),
                        OPEN_EXISTING,
                        FILE_FLAG_OVERLAPPED,
                        ptr::null_mut())
        };

        if handle != INVALID_HANDLE_VALUE {
            return Ok(Handle { value: handle });
        }

        match unsafe { GetLastError() } {
            ERROR_ACCESS_DENIED => handle = unsafe {
                CreateFileW(name.as_ptr(),
                            GENERIC_READ | FILE_WRITE_ATTRIBUTES,
                            0,
                            ptr::null_mut(),
                            OPEN_EXISTING,
                            FILE_FLAG_OVERLAPPED,
                            ptr::null_mut())
            },
            err => return Err(io::Error::from_raw_os_error(err as i32)),
        }

        if handle != INVALID_HANDLE_VALUE {
            return Ok(Handle { value: handle });
        }

        match unsafe { GetLastError() } {
            ERROR_ACCESS_DENIED => handle = unsafe {
                CreateFileW(name.as_ptr(),
                            GENERIC_WRITE | FILE_READ_ATTRIBUTES,
                            0,
                            ptr::null_mut(),
                            OPEN_EXISTING,
                            FILE_FLAG_OVERLAPPED,
                            ptr::null_mut())
            },
            err => return Err(io::Error::from_raw_os_error(err as i32)),
        }

        if handle != INVALID_HANDLE_VALUE {
            Ok(Handle { value: handle })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Will wait for server infinitely.
    pub fn connect<T: AsRef<OsStr>>(name: T) -> io::Result<PipeClient> {
        PipeClient::connect_ms(name, 0xFFFFFFFF)
    }

    /// Will wait for server. Note that `timeout` 0xFFFFFFFF stands for infinite waiting.
    pub fn connect_ms<T: AsRef<OsStr>>(name: T, timeout: u32) -> io::Result<PipeClient> {
        let mut full_name: OsString = name.as_ref().into();
        full_name.push("\x00");
        let full_name = full_name.encode_wide().collect::<Vec<u16>>();
        let mut waited = false;
        loop {
            match PipeClient::create_file(&full_name) {
                Ok(handle) => {
                    let result = unsafe {
                        let mut mode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT;
                        SetNamedPipeHandleState(handle.value,
                                                &mut mode,
                                                ptr::null_mut(),
                                                ptr::null_mut())
                    };

                    if result != 0 {
                        return Ok(PipeClient { handle: handle, ovl: try!(Overlapped::new()) });
                    } else {
                        return Err(io::Error::last_os_error());
                    }
                },
                Err(err) => {
                    if err.raw_os_error().unwrap() == ERROR_PIPE_BUSY as i32 {
                        if ! waited {
                            waited = true;
                            let result = unsafe { WaitNamedPipeW(full_name.as_ptr(), timeout) };
                            if result == 0 {
                                return Err(err);
                            }
                        } else {
                            return Err(err);
                        }
                    } else {
                        return Err(err);
                    }
                },
            }
        }
    }

    /// Initializes asyncronous read operation.
    pub fn read_async<'a, 'b: 'a>(&'a mut self, buf: &'b mut [u8]) -> io::Result<ReadHandle<'a, Self>> {
        init_read(self, buf)
    }

    /// Initializes asyncronous read operation and takes ownership of buffer and client.
    pub fn read_async_owned(self, buf: Vec<u8>) -> io::Result<ReadHandle<'static, Self>> {
        init_read_owned(self, buf)
    }

    /// Initializes asyncronous write operation.
    pub fn write_async<'a, 'b: 'a>(&'a mut self, buf: &'b [u8]) -> io::Result<WriteHandle<'a, Self>> {
        init_write(self, buf)
    }

    /// Initializes asyncronous write operation and takes ownership of buffer and client.
    pub fn write_async_owned(self, buf: Vec<u8>) -> io::Result<WriteHandle<'static, Self>> {
        init_write_owned(self, buf)
    }
}

impl io::Read for PipeClient {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_async(buf).and_then(|read_handle| read_handle.wait()).map(|x| x.0)
    }
}

impl io::Write for PipeClient {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_async(buf).and_then(|write_handle| write_handle.wait()).map(|x| x.0)
    }

    fn flush(&mut self) -> io::Result<()> {
        let result = unsafe { FlushFileBuffers(self.handle.value) };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

pub struct PipeIoObj<'a> {
    handle: HANDLE,
    ovl: &'a mut Overlapped,
}

#[allow(dead_code)]
pub struct PipeIoHandles<'a> {
    pipe_handle: HANDLE,
    event_handle: HANDLE,
    _phantom: PhantomData<&'a ()>
}

/// This trait used for genericity.
pub trait PipeIo {
    fn io_obj<'a>(&'a mut self) -> PipeIoObj<'a>;
    fn io_handles<'a>(&'a self) -> PipeIoHandles<'a>;
}

impl PipeIo for PipeServer {
    fn io_obj<'a>(&'a mut self) -> PipeIoObj<'a> {
        let raw_handle = match self.handle {
            Some(ref handle) => handle.value,
            None => unreachable!(),
        };
        let ovl = match self.ovl {
            Some(ref mut ovl) => ovl,
            None => unreachable!(),
        };
        PipeIoObj {
            handle: raw_handle,
            ovl: ovl,
        }
    }

    fn io_handles<'a>(&'a self) -> PipeIoHandles<'a> {
        let pipe_handle = match self.handle {
            Some(ref handle) => handle.value,
            None => unreachable!(),
        };
        let event_handle = match self.ovl {
            Some(ref ovl) => ovl.ovl.hEvent,
            None => unreachable!(),
        };
        PipeIoHandles {
            pipe_handle: pipe_handle,
            event_handle: event_handle,
            _phantom: PhantomData,
        }
    }
}

impl PipeIo for PipeClient {
    fn io_obj<'a>(&'a mut self) -> PipeIoObj<'a> {
        PipeIoObj {
            handle: self.handle.value,
            ovl: &mut self.ovl,
        }
    }

    fn io_handles<'a>(&'a self) -> PipeIoHandles<'a> {
        PipeIoHandles {
            pipe_handle: self.handle.value,
            event_handle: self.ovl.ovl.hEvent,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: PipeIo> PipeIo for ReadHandle<'a, T> {
    fn io_obj<'b>(&'b mut self) -> PipeIoObj<'b> {
        match self.io {
            Some(ref mut io) => return io.io_obj(),
            _ => ()
        }
        match self.io_ref {
            Some(ref mut io) => return io.io_obj(),
            _ => (),
        }
        unreachable!();
    }

    fn io_handles<'b>(&'b self) -> PipeIoHandles<'b> {
        match self.io {
            Some(ref io) => return io.io_handles(),
            _ => ()
        }
        match self.io_ref {
            Some(ref io) => return io.io_handles(),
            _ => (),
        }
        unreachable!();
    }
}

impl<'a, T: PipeIo> PipeIo for WriteHandle<'a, T> {
    fn io_obj<'b>(&'b mut self) -> PipeIoObj<'b> {
        match self.io {
            Some(ref mut io) => return io.io_obj(),
            _ => ()
        }
        match self.io_ref {
            Some(ref mut io) => return io.io_obj(),
            _ => (),
        }
        unreachable!();
    }

    fn io_handles<'b>(&'b self) -> PipeIoHandles<'b> {
        match self.io {
            Some(ref io) => return io.io_handles(),
            _ => ()
        }
        match self.io_ref {
            Some(ref io) => return io.io_handles(),
            _ => (),
        }
        unreachable!();
    }
}

impl PipeIo for ConnectingServer {
    fn io_obj<'a>(&'a mut self) -> PipeIoObj<'a> {
        PipeIoObj {
            handle: self.handle.value,
            ovl: &mut self.ovl,
        }
    }

    fn io_handles<'a>(&'a self) -> PipeIoHandles<'a> {
        PipeIoHandles {
            pipe_handle: self.handle.value,
            event_handle: self.ovl.ovl.hEvent,
            _phantom: PhantomData,
        }
    }
}

/// Pending read operation. Can be used with [`wait`](fn.wait.html) and [`wait_all`]
/// (fn.wait_all.html) functions.
pub struct ReadHandle<'a, T> {
    io: Option<T>,
    io_ref: Option<&'a mut PipeIo>,
    bytes_read: u32,
    pending: bool,
    buffer: Option<Vec<u8>>,
}

impl<'a, T: PipeIo> ReadHandle<'a, T> {
    /// Will wait infinitely for completion.
    ///
    /// Returns (<bytes_read>, <owned_data>). Owned data is `Some((T, Vec<u8>))` if `ReadHandle`
    /// was created as a result of `T::read_async_owned`.
    pub fn wait(mut self) -> io::Result<(usize, Option<(T, Vec<u8>)>)> {
        if self.pending {
            match try!(wait_for_single_obj(&mut self, INFINITE)) {
                Some(_) => match try!(get_ovl_result(&mut self)) {
                    0 => Err(io::Error::last_os_error()),
                    x => {
                        let ReadHandle { io, io_ref: _, bytes_read: _, pending: _, buffer } = self;
                        if let Some(buf) = buffer {
                            if let Some(io) = io {
                                Ok((x, Some((io, buf))))
                            } else {
                                unreachable!()
                            }
                        } else {
                            Ok((x, None))
                        }
                    },
                },
                None => unreachable!(),
            }
        } else {
            let ReadHandle { io, io_ref: _, bytes_read, pending: _, buffer } = self;
            if let Some(buf) = buffer {
                if let Some(io) = io {
                    Ok((bytes_read as usize, Some((io, buf))))
                } else {
                    unreachable!()
                }
            } else {
                Ok((bytes_read as usize, None))
            }
        }
    }
}

/// Pending write operation. Can be used with [`wait`](fn.wait.html) and [`wait_all`]
/// (fn.wait_all.html) functions.
pub struct WriteHandle<'a, T> {
    buffer: Option<Vec<u8>>,
    io: Option<T>,
    io_ref: Option<&'a mut PipeIo>,
    bytes_written: u32,
    num_bytes: u32,
    pending: bool,
}

impl<'a, T: PipeIo> WriteHandle<'a, T> {
    /// Will wait infinitely for completion.
    ///
    /// Returns (<bytes_read>, <owned_data>). Owned data is `Some((T, Vec<u8>))` if `WriteHandle`
    /// was created as a result of `T::write_async_owned`.
    fn wait(mut self) -> io::Result<(usize, Option<(T, Vec<u8>)>)> {
        if self.pending {
            match try!(wait_for_single_obj(&mut self, INFINITE)) {
                Some(_) => match try!(get_ovl_result(&mut self)) {
                    x if x as u32 == self.num_bytes => {
                        let WriteHandle {
                            io,
                            io_ref: _,
                            bytes_written: _,
                            num_bytes: _,
                            pending: _,
                            buffer } = self;
                        if let Some(buf) = buffer {
                            if let Some(io) = io {
                                Ok((x, Some((io, buf))))
                            } else {
                                unreachable!()
                            }
                        } else {
                            Ok((x, None))
                        }
                    },
                    _ => Err(io::Error::last_os_error()),
                },
                None => unreachable!(),
            }
        } else {
            let WriteHandle {
                io,
                io_ref: _,
                bytes_written,
                num_bytes: _,
                pending: _,
                buffer } = self;
            if let Some(buf) = buffer {
                if let Some(io) = io {
                    Ok((bytes_written as usize, Some((io, buf))))
                } else {
                    unreachable!()
                }
            } else {
                Ok((bytes_written as usize, None))
            }
        }
    }
}

/// Returns `Ok(true)` if connection is pending or `Ok(false)` if pipe is connected.
fn connect_named_pipe(handle: &Handle, ovl: &mut Overlapped) -> io::Result<bool> {
    let result = unsafe { ConnectNamedPipe(handle.value, ovl.get_mut()) };
    if result == TRUE {
        // Overlapped ConnectNamedPipe should return FALSE
        return Err(io::Error::last_os_error())
    } else {
        let err = io::Error::last_os_error();
        let mut pending = false;
        match err.raw_os_error().unwrap() as u32 {
            ERROR_IO_PENDING => pending = true,
            ERROR_PIPE_CONNECTED => try!(ovl.event.set()),
            _ => return Err(err),
        }
        Ok(pending)
    }
}

fn init_read<'a, 'b: 'a, T>(this: &'a mut T, buf: &'b mut [u8]) -> io::Result<ReadHandle<'a, T>>
where T: PipeIo {
    let mut bytes_read = 0;
    let result = unsafe {
        let io_obj = this.io_obj();
        ReadFile(io_obj.handle,
                 buf.as_mut_ptr() as *mut c_void,
                 buf.len() as u32,
                 &mut bytes_read,
                 &mut io_obj.ovl.ovl)
    };

    if result != 0 && bytes_read != 0 {
        Ok(ReadHandle {
            io: None,
            io_ref: Some(this),
            bytes_read: bytes_read,
            pending: false,
            buffer: None,
        })
    } else {
        let err = io::Error::last_os_error();
        if result == 0 && err.raw_os_error().unwrap() == ERROR_IO_PENDING as i32 {
            Ok(ReadHandle {
                io: None,
                io_ref: Some(this),
                bytes_read: 0,
                pending: true,
                buffer: None,
            })
        } else {
            Err(err)
        }
    }
}

fn init_read_owned<T: PipeIo>(mut this: T, mut buf: Vec<u8>) -> io::Result<ReadHandle<'static, T>> {
    let mut bytes_read = 0;
    let result = unsafe {
        let io_obj = this.io_obj();
        ReadFile(io_obj.handle,
                 buf.as_mut_ptr() as *mut c_void,
                 buf.len() as u32,
                 &mut bytes_read,
                 &mut io_obj.ovl.ovl)
    };

    if result != 0 && bytes_read != 0 {
        Ok(ReadHandle {
            io: Some(this),
            io_ref: None,
            bytes_read: bytes_read,
            pending: false,
            buffer: Some(buf),
        })
    } else {
        let err = io::Error::last_os_error();
        if result == 0 && err.raw_os_error().unwrap() == ERROR_IO_PENDING as i32 {
            Ok(ReadHandle {
                io: Some(this),
                io_ref: None,
                bytes_read: 0,
                pending: true,
                buffer: Some(buf),
            })
        } else {
            Err(err)
        }
    }
}

fn init_write<'a, 'b: 'a, T>(this: &'a mut T, buf: &'b [u8]) -> io::Result<WriteHandle<'a, T>>
where T: PipeIo {
    assert!(buf.len() <= 0xFFFFFFFF);
    let mut bytes_written = 0;
    let result = unsafe {
        let io_obj = this.io_obj();
        WriteFile(io_obj.handle,
                  buf.as_ptr() as *mut c_void,
                  buf.len() as u32,
                  &mut bytes_written,
                  &mut io_obj.ovl.ovl)
    };

    if result != 0 && bytes_written == buf.len() as u32 {
        Ok(WriteHandle {
            io: None,
            io_ref: Some(this),
            buffer: None,
            bytes_written: bytes_written,
            num_bytes: buf.len() as u32,
            pending: false,
        })
    } else {
        let err = io::Error::last_os_error();
        if result == 0 && err.raw_os_error().unwrap() == ERROR_IO_PENDING as i32 {
            Ok(WriteHandle {
                io: None,
                io_ref: Some(this),
                buffer: None,
                bytes_written: 0,
                num_bytes: buf.len() as u32,
                pending: true,
            })
        } else {
            Err(err)
        }
    }
}

fn init_write_owned<'a, 'b: 'a, T>(mut this: T, buf: Vec<u8>) -> io::Result<WriteHandle<'static, T>>
where T: PipeIo {
    assert!(buf.len() <= 0xFFFFFFFF);
    let mut bytes_written = 0;
    let result = unsafe {
        let io_obj = this.io_obj();
        WriteFile(io_obj.handle,
                  buf.as_ptr() as *mut c_void,
                  buf.len() as u32,
                  &mut bytes_written,
                  &mut io_obj.ovl.ovl)
    };

    if result != 0 && bytes_written == buf.len() as u32 {
        Ok(WriteHandle {
            io_ref: None,
            io: Some(this),
            num_bytes: buf.len() as u32,
            buffer: Some(buf),
            bytes_written: bytes_written,
            pending: false,
        })
    } else {
        let err = io::Error::last_os_error();
        if result == 0 && err.raw_os_error().unwrap() == ERROR_IO_PENDING as i32 {
            Ok(WriteHandle {
                io_ref: None,
                io: Some(this),
                num_bytes: buf.len() as u32,
                buffer: Some(buf),
                bytes_written: 0,
                pending: true,
            })
        } else {
            Err(err)
        }
    }
}

fn get_ovl_result<T: PipeIo>(this: &mut T) -> io::Result<usize> {
    let mut count = 0;
    let result = unsafe {
        let io_obj = this.io_obj();
        GetOverlappedResult(io_obj.handle,
                            &mut io_obj.ovl.ovl,
                            &mut count,
                            TRUE)
    };
    if result != 0 {
        Ok(count as usize)
    } else {
        Err(io::Error::last_os_error())
    }
}

fn wait_for_single_obj<T>(this: &mut T, timeout: u32) -> io::Result<Option<usize>>
where T: PipeIo {
    let result = unsafe {
        let io_obj = this.io_obj();
        WaitForSingleObject(io_obj.ovl.event.handle.value,
                            timeout)
    };

    match result {
        WAIT_OBJECT_0 => Ok(Some(0)),
        WAIT_TIMEOUT => Ok(None),
        WAIT_FAILED => Err(io::Error::last_os_error()),
        _ => unreachable!(),
    }
}

fn wait_for_multiple_obj<T>(list: &[T], all: bool, timeout: u32) -> io::Result<Option<usize>>
where T: PipeIo {
    assert!(list.len() <= MAXIMUM_WAIT_OBJECTS as usize);
    if list.len() == 0 {
        Ok(None)
    } else {
        let mut events = Vec::with_capacity(list.len());

        for obj in list.iter() {
            events.push(obj.io_handles().event_handle);
        }

        let result = unsafe {
            WaitForMultipleObjects(events.len() as u32,
                                   events.as_ptr(),
                                   if all { TRUE } else { FALSE },
                                   timeout)
        };

        if all {
            match result {
                WAIT_OBJECT_0 => Ok(Some(0)),
                WAIT_TIMEOUT => Ok(None),
                WAIT_FAILED => Err(io::Error::last_os_error()),
                _ => unreachable!(),
            }
        } else {
            match result {
                i if i < 64 => Ok(Some(i as usize)),
                WAIT_TIMEOUT => Ok(None),
                WAIT_FAILED => Err(io::Error::last_os_error()),
                _ => unreachable!(),
            }
        }
    }
}

/// This function will wait for first finished io operation and return it's index in `list`.
///
/// # Panics
///
/// This function will panic if `list.len() == 0` or `list.len() > MAXIMUM_WAIT_OBJECTS`
pub fn wait<T: PipeIo>(list: &[T]) -> io::Result<usize> {
    assert!(list.len() > 0);

    match try!(wait_for_multiple_obj(list, false, INFINITE)) {
        Some(x) => Ok(x),
        None => unreachable!(),
    }
}

/// This function will wait for all overlapped io operations to finish.
///
/// # Panics
///
/// This function will panic if `list.len() == 0` or `list.len() > MAXIMUM_WAIT_OBJECTS`
pub fn wait_all<T: PipeIo>(list: &[T]) -> io::Result<()> {
    assert!(list.len() > 0);

    if try!(wait_for_multiple_obj(list, true, INFINITE)).is_some() {
        Ok(())
    } else {
        unreachable!()
    }
}

#[test]
fn test_io_single_thread() {
    let connecting_server = PipeOptions::new(r"\\.\pipe\test_io_single_thread").single().unwrap();
    let mut client = PipeClient::connect(r"\\.\pipe\test_io_single_thread").unwrap();
    let mut server = connecting_server.wait().unwrap();
    {
        let w_handle = server.write_async(b"0123456789").unwrap();
        {
            let mut buf = [0; 5];
            {
                let r_handle = client.read_async(&mut buf[..]).unwrap();
                r_handle.wait().unwrap();
            }
            assert_eq!(&buf[..], b"01234");
            {
                let r_handle = client.read_async(&mut buf[..]).unwrap();
                r_handle.wait().unwrap();
            }
            assert_eq!(&buf[..], b"56789");
        }
        w_handle.wait().unwrap();
    }

    let connecting_server = server.unwrap().unwrap();
    let mut client = PipeClient::connect(r"\\.\pipe\test_io_single_thread").unwrap();
    let mut server = connecting_server.wait().unwrap();
    {
        let w_handle = server.write_async(b"0123456789").unwrap();
        {
            let mut buf = [0; 5];
            {
                let r_handle = client.read_async(&mut buf[..]).unwrap();
                r_handle.wait().unwrap();
            }
            assert_eq!(&buf[..], b"01234");
            {
                let r_handle = client.read_async(&mut buf[..]).unwrap();
                r_handle.wait().unwrap();
            }
            assert_eq!(&buf[..], b"56789");
        }
        w_handle.wait().unwrap();
    }
}

#[test]
fn test_io_multiple_threads() {
    use std::thread;
    use std::io::{Read, Write};

    let connecting_server = PipeOptions::new(r"\\.\pipe\test_io_multiple_threads").single().unwrap();
    let t1 = thread::spawn(move || {
        let mut buf = [0; 5];
        let mut client = PipeClient::connect(r"\\.\pipe\test_io_multiple_threads").unwrap();
        client.read(&mut buf).unwrap();
        client.write(b"done").unwrap();
        buf
    });
    let t2 = thread::spawn(move || {
        thread::sleep_ms(50);
        let mut buf = [0; 5];
        let mut client = PipeClient::connect(r"\\.\pipe\test_io_multiple_threads").unwrap();
        client.read(&mut buf).unwrap();
        client.write(b"done").unwrap();
        buf
    });

    let mut buf = [0; 4];
    let mut server = connecting_server.wait().unwrap();
    server.write(b"01234").unwrap();
    server.read(&mut buf).unwrap();
    assert_eq!(b"done", &buf[..]);

    let mut buf = [0; 4];
    let mut server = server.unwrap().unwrap().wait().unwrap();
    server.write(b"56789").unwrap();
    server.read(&mut buf).unwrap();
    assert_eq!(b"done", &buf[..]);
    server.unwrap().unwrap();

    assert_eq!(b"01234", &t1.join().unwrap()[..]);
    assert_eq!(b"56789", &t2.join().unwrap()[..]);
}

#[test]
fn test_wait() {
    use std::thread;
    use std::io::{Read, Write};

    let mut servers = PipeOptions::new(r"\\.\pipe\test_wait").multiple(16).unwrap();
    let t1 = thread::spawn(move || {
        for _ in 0..16 {
            let mut buf = [0; 10];
            let mut client = PipeClient::connect(r"\\.\pipe\test_wait").unwrap();
            client.read(&mut buf).unwrap();
            client.write(b"done").unwrap();
            assert_eq!(b"0123456789", &buf[..]);
        }
    });

    while servers.len() > 0 {
        let mut buf = [0; 4];
        let which = wait(servers.as_ref()).unwrap();
        let mut server = servers.remove(which).wait().unwrap();
        server.write(b"0123456789").unwrap();
        server.read(&mut buf).unwrap();
        assert_eq!(b"done", &buf[..]);
    }

    t1.join().unwrap();
}

#[test]
fn test_wait_all() {
    use std::thread;

    let connecting_servers = PipeOptions::new(r"\\.\pipe\test_wait_all").multiple(16).unwrap();
    let t1 = thread::spawn(move || {
        let mut clients = Vec::with_capacity(16);
        for _ in 0..16 {
            clients.push(PipeClient::connect(r"\\.\pipe\test_wait_all").unwrap());
        }
        let read_handles = clients.into_iter()
                                  .map(|c| c.read_async_owned(vec![0; 4]).unwrap())
                                  .collect::<Vec<ReadHandle<'static, PipeClient>>>();
        wait_all(read_handles.as_ref()).unwrap();
        let (results, clients) = read_handles.into_iter().fold((vec![], vec![]), |mut vecs, x| {
            if let (count, Some((client, buf))) = x.wait().unwrap() {
                assert_eq!(count, 4);
                vecs.0.push(buf);
                vecs.1.push(client);
                vecs
            } else {
                unreachable!();
            }
        });
        for (i, result) in results.into_iter().enumerate() {
            assert_eq!(vec![i as u8, i as u8, i as u8, i as u8], result);
        }

        let write_handles = clients.into_iter()
                                   .map(|c| c.write_async_owned(b"done".to_vec()).unwrap())
                                   .collect::<Vec<WriteHandle<'static, PipeClient>>>();
        wait_all(write_handles.as_ref()).unwrap();
        let it = write_handles.into_iter().map(|x| {
            if let (count, Some(_)) = x.wait().unwrap() {
                assert_eq!(count, 4);
            } else {
                unreachable!();
            }
        });
        for _ in it {}
    });

    wait_all(connecting_servers.as_ref()).unwrap();
    let servers = connecting_servers.into_iter().map(|cs| cs.wait().unwrap());

    let write_handles = servers.into_iter().enumerate()
                               .map(|(i, s)| s.write_async_owned(vec![i as u8; 4]).unwrap())
                               .collect::<Vec<WriteHandle<'static, PipeServer>>>();
    wait_all(write_handles.as_ref()).unwrap();
    let servers = write_handles.into_iter().fold(Vec::new(), |mut ss, x| {
        if let (count, Some((s, _))) = x.wait().unwrap() {
            assert_eq!(count, 4);
            ss.push(s);
            ss
        } else {
            unreachable!();
        }
    });

    let read_handles = servers.into_iter()
                              .map(|s| s.read_async_owned(vec![0; 4]).unwrap())
                              .collect::<Vec<ReadHandle<'static, PipeServer>>>();
    wait_all(read_handles.as_ref()).unwrap();
    let _ = read_handles.into_iter().map(|x| {
        if let (count, Some((_, buf))) = x.wait().unwrap() {
            assert_eq!(count, 4);
            assert_eq!(b"done".to_vec(), buf);
        } else {
            unreachable!()
        }
    });

    t1.join().unwrap();
}
