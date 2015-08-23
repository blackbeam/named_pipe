extern crate kernel32;
extern crate winapi;

use std::ptr;
use std::thread;
use std::ffi::OsString;
use std::os::raw::c_void;
use std::os::windows::ffi::OsStrExt;

use winapi::INVALID_HANDLE_VALUE;
use winapi::fileapi::OPEN_EXISTING;
use winapi::winbase::{
    PIPE_ACCESS_OUTBOUND,
    PIPE_TYPE_BYTE,
};
use winapi::winnt::{
    HANDLE,
    GENERIC_READ,
    FILE_SHARE_READ,
    FILE_SHARE_WRITE,
    FILE_ATTRIBUTE_NORMAL,
};

use kernel32::{
    ReadFile,
    WriteFile,
    CreateFileW,
    CloseHandle,
    GetLastError,
    CreateNamedPipeW,
    ConnectNamedPipe,
};

#[test]
fn it_works() {
    use std::sync::mpsc::channel;
    let (tx, rx) = channel();

    let os_pipe_name: OsString = String::from("\\\\.\\pipe\\my_pipe\x00").into();
    let os_pipe_name: Vec<u16> = os_pipe_name.encode_wide().collect::<Vec<u16>>();
    let os_pipe_name_clone = os_pipe_name.clone();

    let t_handle = thread::spawn(move || {
        let handle: HANDLE = unsafe {
            CreateNamedPipeW(os_pipe_name_clone.as_ptr(),
                             PIPE_ACCESS_OUTBOUND,
                             PIPE_TYPE_BYTE,
                             1,
                             0,
                             0,
                             0,
                             ptr::null_mut())
        };

        if handle == ptr::null_mut() || handle == INVALID_HANDLE_VALUE {
            unsafe { panic!("{:?}", GetLastError()); }
        }

        tx.send(false).unwrap();
        let result = unsafe {
            ConnectNamedPipe(handle, ptr::null_mut())
        };

        if result == 0 {
            unsafe { panic!("{:?}", GetLastError()); }
        }

        let msg: OsString = String::from("*** Hello Pipe World ***").into();
        let msg: Vec<u16> = msg.encode_wide().collect();
        let mut num_bytes_written = 0;

        let result = unsafe {
            WriteFile(handle,
                      msg.as_ptr() as *const c_void,
                      msg.len() as u32 * 2,
                      &mut num_bytes_written,
                      ptr::null_mut())
        };

        if result == 0 {
            unsafe { panic!("{:?}", GetLastError()); }
        }

        unsafe { CloseHandle(handle); }
    });

    rx.recv().unwrap();
    thread::sleep_ms(1000);

    let handle = unsafe {
        CreateFileW(os_pipe_name.as_ptr(),
                    GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    ptr::null_mut(),
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    ptr::null_mut())
    };

    if handle == ptr::null_mut() || handle == INVALID_HANDLE_VALUE {
        unsafe { panic!("{:?}", GetLastError()); }
    }

    let mut buffer = vec![0u16; 128];
    let mut num_bytes_read = 0;
    let result = unsafe {
        ReadFile(handle,
                 buffer.as_mut_ptr() as *mut c_void,
                 127 * 2,
                 &mut num_bytes_read,
                 ptr::null_mut())
    };

    if result == 0 {
        unsafe { panic!("{:?} {:?}", INVALID_HANDLE_VALUE, GetLastError()); }
    }

    unsafe { CloseHandle(handle); }

    assert!(t_handle.join().is_ok());
}
