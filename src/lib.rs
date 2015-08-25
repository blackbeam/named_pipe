extern crate kernel32;
extern crate winapi;

use std::io::{
    self,
    Read,
    Write,
};
use std::ptr;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::os::raw::c_void;
use std::os::windows::ffi::OsStrExt;

use winapi::INVALID_HANDLE_VALUE;
use winapi::fileapi::OPEN_EXISTING;
use winapi::winbase::{
    INFINITE,
    PIPE_WAIT,
    WAIT_OBJECT_0,
    PIPE_TYPE_BYTE,
    PIPE_READMODE_BYTE,
    PIPE_ACCESS_DUPLEX,
    FILE_FLAG_OVERLAPPED,
    PIPE_UNLIMITED_INSTANCES,
    FILE_FLAG_FIRST_PIPE_INSTANCE,
};
use winapi::winnt::{
    HANDLE,
    GENERIC_READ,
    GENERIC_WRITE,
    MAXIMUM_WAIT_OBJECTS,
    FILE_READ_ATTRIBUTES,
    FILE_WRITE_ATTRIBUTES,
};
use winapi::winerror::{
    ERROR_PIPE_BUSY,
    ERROR_IO_PENDING,
    ERROR_ACCESS_DENIED,
    ERROR_PIPE_CONNECTED,
};
use winapi::minwinbase::{
    OVERLAPPED,
};
use winapi::minwindef::{
    TRUE,
    FALSE,
    DWORD,
};

use kernel32::{
    SetEvent,
    ReadFile,
    WriteFile,
    ResetEvent,
    CreateFileW,
    CloseHandle,
    CreateEventW,
    GetLastError,
    WaitNamedPipeW,
    CreateNamedPipeW,
    ConnectNamedPipe,
    FlushFileBuffers,
    GetOverlappedResult,
    DisconnectNamedPipe,
    WaitForMultipleObjects,
    SetNamedPipeHandleState,
};

fn create_file(os_pipe_name: *const u16) -> io::Result<HANDLE> {
    let mut handle = unsafe {
        CreateFileW(os_pipe_name,
                    GENERIC_READ | GENERIC_WRITE,
                    0,
                    ptr::null_mut(),
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED,
                    ptr::null_mut())
    };

    if handle == INVALID_HANDLE_VALUE {
        match unsafe { GetLastError() } {
            ERROR_ACCESS_DENIED => {
                handle = unsafe {
                    CreateFileW(os_pipe_name,
                                GENERIC_READ | FILE_WRITE_ATTRIBUTES,
                                0,
                                ptr::null_mut(),
                                OPEN_EXISTING,
                                FILE_FLAG_OVERLAPPED,
                                ptr::null_mut())
                };
            },
            err => return Err(io::Error::from_raw_os_error(err as i32)),
        }
    }

    if handle == INVALID_HANDLE_VALUE {
        match unsafe { GetLastError() } {
            ERROR_ACCESS_DENIED => {
                handle = unsafe {
                    CreateFileW(os_pipe_name,
                                GENERIC_WRITE | FILE_READ_ATTRIBUTES,
                                0,
                                ptr::null_mut(),
                                OPEN_EXISTING,
                                FILE_FLAG_OVERLAPPED,
                                ptr::null_mut())
                };
            },
            err => return Err(io::Error::from_raw_os_error(err as i32)),
        }
    }

    if handle == INVALID_HANDLE_VALUE {
        Err(io::Error::last_os_error())
    } else {
        Ok(handle)
    }
}

fn create_event() -> io::Result<HANDLE> {
    let handle = unsafe {
        CreateEventW(ptr::null_mut(),
                     1,
                     0,
                     ptr::null())
    };
    if handle == ptr::null_mut() {
        return Err(io::Error::last_os_error())
    } else {
        Ok(handle)
    }
}

fn read(pipe_handle: HANDLE, buf: &mut [u8]) -> io::Result<usize> {
    let event = try!(create_event());
    let mut bytes_read = 0;
    let mut overlapped = OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Offset: 0,
        OffsetHigh: 0,
        hEvent: event,
    };
    let result = unsafe {
        ReadFile(pipe_handle,
                 buf.as_mut_ptr() as *mut c_void,
                 buf.len() as DWORD,
                 &mut bytes_read,
                 &mut overlapped)
    };
    if result == 0 {
        let err = unsafe { GetLastError() };
        if err == ERROR_IO_PENDING {
            let result = unsafe {
                GetOverlappedResult(pipe_handle,
                                    &mut overlapped,
                                    &mut bytes_read,
                                    TRUE)
            };
            if result == 0 {
                return Err(io::Error::last_os_error());
            }
        } else {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }

    Ok(bytes_read as usize)
}

fn write(pipe_handle: HANDLE, buf: &[u8]) -> io::Result<usize> {
    let event = try!(create_event());
    let mut offset = 0;
    let mut overlapped = OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Offset: 0,
        OffsetHigh: 0,
        hEvent: event,
    };
    while offset < buf.len() {
        let mut bytes_written = 0;
        let result = unsafe {
            WriteFile(pipe_handle,
                      (&buf[offset..]).as_ptr() as *const c_void,
                      (buf.len() - offset) as u32,
                      &mut bytes_written,
                      &mut overlapped)
        };
        if result == 0 {
            let err = unsafe { GetLastError() };
            if err == ERROR_IO_PENDING {
                let result = unsafe {
                    GetOverlappedResult(pipe_handle,
                                        &mut overlapped,
                                        &mut bytes_written,
                                        TRUE)
                };
                if result == 0 {
                    return Err(io::Error::last_os_error());
                }
            } else {
                return Err(io::Error::from_raw_os_error(err as i32));
            }
        }
        offset += bytes_written as usize;
    }

    Ok(buf.len())
}

struct Event {
    event: HANDLE,
}

impl Drop for Event {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.event);
        }
    }
}

pub struct Pipe {
    handle: HANDLE,
    full_name: Vec<u16>,
    connect_event: Option<Event>,
    connecting: bool,
}

impl Pipe {
    fn raw_new_instance(name: *const u16, new: bool) -> io::Result<HANDLE> {
        let handle = unsafe {
            CreateNamedPipeW(name,
                             (PIPE_ACCESS_DUPLEX |
                                FILE_FLAG_OVERLAPPED |
                                if new {FILE_FLAG_FIRST_PIPE_INSTANCE} else {0}),
                             PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                             PIPE_UNLIMITED_INSTANCES,
                             65536,
                             65536,
                             0,
                             ptr::null_mut())
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        Ok(handle)
    }

    /// Creates new pipe
    pub fn new<T: AsRef<OsStr>>(name: T) -> io::Result<Pipe> {
        let mut full_name: OsString = name.as_ref().into();
        full_name.push("\x00");
        let full_name = full_name.encode_wide().collect::<Vec<u16>>();

        Ok(Pipe {
            handle: try!(Pipe::raw_new_instance(full_name.as_ptr(), true)),
            full_name: full_name,
            connect_event: None,
            connecting: false,
        })
    }

    /// Creates new Pipe with `n` instances
    pub fn new_n<T: AsRef<OsStr>>(name: T, n: usize) -> io::Result<Vec<Pipe>> {
        let mut full_name: OsString = name.as_ref().into();
        full_name.push("\x00");
        let full_name = full_name.encode_wide().collect::<Vec<u16>>();

        let mut out = Vec::with_capacity(n);
        let mut first = true;

        for _ in 0..n {
            let handle = try!(Pipe::raw_new_instance(full_name.as_ptr(), first));
            first = false;
            out.push(
                Pipe {
                    handle: handle,
                    full_name: full_name.clone(),
                    connect_event: None,
                    connecting: false,
                }
            );
        }

        Ok(out)
    }

    /// Creates new instance of existing pipe
    pub fn new_instance<T: AsRef<OsStr>>(name: T) -> io::Result<Pipe> {
        let mut full_name: OsString = name.as_ref().into();
        full_name.push("\x00");
        let full_name = full_name.encode_wide().collect::<Vec<u16>>();

        Ok(Pipe {
            handle: try!(Pipe::raw_new_instance(full_name.as_ptr(), false)),
            full_name: full_name,
            connect_event: None,
            connecting: false,
        })
    }

    /// Creates another `n` instances of this pipe
    pub fn more(&self, n: usize) -> io::Result<Vec<Pipe>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let handle = try!(Pipe::raw_new_instance(self.full_name.as_ptr(), false));
            out.push(
                Pipe {
                    handle: handle,
                    full_name: self.full_name.clone(),
                    connect_event: None,
                    connecting: false,
                }
            );
        }

        Ok(out)
    }

    /// Wait while client connects to this pipe
    pub fn wait_for_client(mut self) -> io::Result<PipeServer> {
        if self.connect_event.is_none() {
            self.connect_event = Some(Event{ event: try!(create_event()) });
        }
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Offset: 0,
            OffsetHigh: 0,
            hEvent: self.connect_event.as_ref().unwrap().event,
        };
        let result = unsafe {
            ConnectNamedPipe(self.handle, &mut overlapped)
        };

        if result == 0 {
            self.connecting = true;
            let err = unsafe { GetLastError() };
            let mut dummy = 0;
            match err {
                ERROR_IO_PENDING => {
                    let result = unsafe {
                        GetOverlappedResult(self.handle,
                                            &mut overlapped,
                                            &mut dummy,
                                            TRUE)
                    };
                    if result == 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(PipeServer { pipe: Some(self) })
                    }
                },
                ERROR_PIPE_CONNECTED => Ok(PipeServer { pipe: Some(self) }),
                err => {
                    self.connecting = false;
                    return Err(io::Error::from_raw_os_error(err as i32));
                }
            }
        } else {
            return Err(io::Error::last_os_error());
        }
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.handle);
        }
    }
}

/// Will wait until client connects to one of pipes, then remove this pipe from pipes and return as
/// `PipeServer` instance. Note that pipes.len() must be less than or equal to
/// `winapi::winnt::MAXIMUM_WAIT_OBJECTS`.
pub fn wait_for_client(pipes: &mut Vec<Pipe>) -> io::Result<PipeServer> {
    assert!(pipes.len() <= MAXIMUM_WAIT_OBJECTS as usize);
    let mut events = Vec::with_capacity(pipes.len());
    for pipe in pipes.iter_mut() {
        if pipe.connect_event.is_none() {
            pipe.connect_event = Some(Event{ event: try!(create_event()) })
        }
        events.push(pipe.connect_event.as_ref().unwrap().event);
    }

    let mut ovls = Vec::with_capacity(pipes.len());
    for i in 0..pipes.len() {
        ovls.push(OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Offset: 0,
            OffsetHigh: 0,
            hEvent: events[i],
        });
    }

    for (i, pipe) in pipes.iter_mut().enumerate() {
        if ! pipe.connecting {
            let result = unsafe {
                ConnectNamedPipe(pipe.handle, &mut ovls[i])
            };

            if result != 0 {
                return Err(io::Error::last_os_error());
            }

            pipe.connecting = true;

            match unsafe { GetLastError() } {
                ERROR_IO_PENDING => (),
                ERROR_PIPE_CONNECTED => unsafe { SetEvent(events[i]); },
                err => {
                    pipe.connecting = false;
                    return Err(io::Error::from_raw_os_error(err as i32));
                }
            }
        }
    }

    let which = unsafe {
        WaitForMultipleObjects(pipes.len() as u32,
                               events.as_ptr(),
                               FALSE,
                               INFINITE)
    };

    let which = (which - WAIT_OBJECT_0) as usize;
    if which >= pipes.len() {
        return Err(io::Error::new(io::ErrorKind::Other, "pipe index out of range"));
    }

    let pipe = pipes.remove(which);
    let mut dummy = 0;
    let result = unsafe {
        GetOverlappedResult(pipe.handle,
                            &mut ovls[which],
                            &mut dummy,
                            TRUE)
    };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(PipeServer { pipe: Some(pipe) })
    }
}

pub struct PipeServer {
    pipe: Option<Pipe>,
}

impl PipeServer {
    /// Unwraps `Pipe`. This method will flush file buffers and disconnect named pipe from client.
    pub fn unwrap(mut self) -> io::Result<Pipe> {
        let mut pipe = self.pipe.take().unwrap();
        let mut result = unsafe {
            FlushFileBuffers(pipe.handle)
        };
        if result != 0 {
            result = unsafe {
                DisconnectNamedPipe(pipe.handle)
            };
        }

        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            result = unsafe {
                ResetEvent(pipe.connect_event.as_ref().unwrap().event)
            };

            if result == 0 {
                Err(io::Error::last_os_error())
            } else {
                pipe.connecting = false;
                Ok(pipe)
            }
        }
    }
}

impl io::Read for PipeServer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pipe.is_some() {
            let handle = self.pipe.as_ref().unwrap().handle;
            read(handle, buf)
        } else {
            unreachable!("self.pipe must not be None on existing PipeServer");
        }
    }
}

impl io::Write for PipeServer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.pipe.is_some() {
            let handle = self.pipe.as_ref().unwrap().handle;
            write(handle, buf)
        } else {
            unreachable!("self.pipe must not be None on existing PipeServer");
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.pipe.is_some() {
            let handle = self.pipe.as_ref().unwrap().handle;
            let result = unsafe {
                FlushFileBuffers(handle)
            };
            if result == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        } else {
            unreachable!("self.pipe must not be None on existing PipeServer");
        }
    }
}

impl Drop for PipeServer {
    fn drop(&mut self) {
        if self.pipe.is_some() {
            let handle = self.pipe.as_ref().unwrap().handle;
            unsafe {
                let _ = FlushFileBuffers(handle);
                let _ = DisconnectNamedPipe(handle);
            }
        }
    }
}

pub struct PipeClient {
    handle: HANDLE,
}

impl PipeClient {
    pub fn connect<T: AsRef<OsStr>>(name: T) -> io::Result<PipeClient> {
        let mut full_name: OsString = name.as_ref().into();
        full_name.push("\x00");
        let full_name = full_name.encode_wide().collect::<Vec<u16>>();
        loop {
            match create_file(full_name.as_ptr()) {
                Ok(handle) => {
                    let result = unsafe {
                        let mut mode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT;
                        SetNamedPipeHandleState(handle,
                                                &mut mode,
                                                ptr::null_mut(),
                                                ptr::null_mut())
                    };
                    if result == 0 {
                        return Err(io::Error::last_os_error());
                    }
                    return Ok(PipeClient { handle: handle });
                },
                Err(err) => {
                    if err.raw_os_error().unwrap() == ERROR_PIPE_BUSY as i32 {
                        if unsafe { WaitNamedPipeW(full_name.as_ptr(), 25000) } == 0 {
                            return Err(io::Error::last_os_error());
                        }
                    } else {
                        return Err(err);
                    }
                },
            }
        }
    }
}

impl io::Read for PipeClient {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        read(self.handle, buf)
    }
}

impl io::Write for PipeClient {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        write(self.handle, buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        let result = unsafe {
            FlushFileBuffers(self.handle)
        };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[test]
fn should_create_new_pipe() {
    Pipe::new(r"\\.\pipe\mypipe").unwrap();
}

#[test]
fn should_create_n_new_pipes() {
    Pipe::new_n(r"\\.\pipe\mypipe", 32).unwrap();
}

#[test]
fn should_create_another_instances_of_created_pipe() {
    let pipe = Pipe::new(r"\\.\pipe\mypipe").unwrap();
    pipe.more(32).unwrap();
}

#[test]
#[allow(unused_variables)]
fn should_create_another_instances_of_existing_pipe() {
    let pipe = Pipe::new(r"\\.\pipe\mypipe").unwrap();
    let mut insts = Vec::with_capacity(32);
    for _ in 0..32 {
        insts.push(Pipe::new_instance(r"\\.\pipe\mypipe").unwrap());
    }
}

#[test]
#[allow(unused_variables)]
fn should_wait_for_client() {
    use std::thread;

    let t1 = thread::spawn(move || {
        thread::sleep_ms(100);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    let t2 = thread::spawn(move || {
        thread::sleep_ms(100);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    {
        let pipe1 = Pipe::new(r"\\.\pipe\mypipe").unwrap();
        let pipe2 = Pipe::new_instance(r"\\.\pipe\mypipe").unwrap();
        let s1 = pipe1.wait_for_client().unwrap();
        let s2 = pipe2.wait_for_client().unwrap();
        thread::sleep_ms(100);
    }

    assert!(t1.join().is_ok());
    assert!(t2.join().is_ok());
}

#[test]
#[allow(unused_variables)]
fn should_wait_for_multiple_clients() {
    use std::thread;

    let t1 = thread::spawn(move || {
        thread::sleep_ms(100);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    let t2 = thread::spawn(move || {
        thread::sleep_ms(100);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    {
        let mut pipes = Pipe::new_n(r"\\.\pipe\mypipe", 2).unwrap();
        let s1 = wait_for_client(&mut pipes).unwrap();
        let s2 = wait_for_client(&mut pipes).unwrap();
        thread::sleep_ms(100);
    }

    assert!(t1.join().is_ok());
    assert!(t2.join().is_ok());
}

#[test]
#[allow(unused_variables)]
fn should_be_able_to_unwrap_pipe_from_a_pipe_server() {
    use std::thread;

    let t1 = thread::spawn(move || {
        thread::sleep_ms(100);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    let t2 = thread::spawn(move || {
        thread::sleep_ms(100);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    let t3 = thread::spawn(move || {
        thread::sleep_ms(300);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    let t4 = thread::spawn(move || {
        thread::sleep_ms(300);
        assert!(PipeClient::connect(r"\\.\pipe\mypipe").is_ok());
    });

    {
        let mut pipes = Pipe::new_n(r"\\.\pipe\mypipe", 2).unwrap();
        let s1 = wait_for_client(&mut pipes).unwrap();
        let s2 = wait_for_client(&mut pipes).unwrap();
        thread::sleep_ms(100);
        let mut pipes = vec![s1.unwrap().unwrap(), s2.unwrap().unwrap()];
        let s1 = wait_for_client(&mut pipes).unwrap();
        let s2 = wait_for_client(&mut pipes).unwrap();
        thread::sleep_ms(100);
    }

    assert!(t1.join().is_ok());
    assert!(t2.join().is_ok());
}

#[test]
fn should_read_and_write_from_pipe_ends() {
    use std::thread;
    let t1 = thread::spawn(move || {
        thread::sleep_ms(100);
        let mut client = PipeClient::connect(r"\\.\pipe\mypipe").unwrap();
        assert_eq!(10, client.write(b"0123456789").unwrap());
        let mut buf = [0; 20];
        assert_eq!(10, client.read(&mut buf[..]).unwrap());
        assert_eq!(&buf[..], b"0123456789\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
    });

    {
        let pipe = Pipe::new(r"\\.\pipe\mypipe").unwrap();
        let mut server = pipe.wait_for_client().unwrap();
        let mut buf = [0; 20];
        assert_eq!(10, server.read(&mut buf[..]).unwrap());
        assert_eq!(&buf[..], b"0123456789\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
        assert_eq!(10, server.write(b"0123456789").unwrap());
    }

    assert!(t1.join().is_ok());
}
