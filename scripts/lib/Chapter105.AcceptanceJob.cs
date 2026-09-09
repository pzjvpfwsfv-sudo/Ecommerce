using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Chapter105
{
    public sealed class AcceptanceJobResult
    {
        public int ExitCode { get; set; }
        public bool TimedOut { get; set; }
        public string StandardOutput { get; set; }
        public string StandardError { get; set; }
    }

    public static class AcceptanceJobRunner
    {
        private const uint CreateSuspended = 0x00000004;
        private const uint CreateNoWindow = 0x08000000;
        private const uint StartfUseStdHandles = 0x00000100;
        private const uint HandleFlagInherit = 0x00000001;
        private const uint JobObjectExtendedLimitInformationClass = 9;
        private const uint JobObjectLimitKillOnJobClose = 0x00002000;
        private const uint WaitObject0 = 0x00000000;
        private const uint WaitTimeout = 0x00000102;
        private const uint Infinite = 0xffffffff;

        [StructLayout(LayoutKind.Sequential)]
        private struct SecurityAttributes
        {
            public int Length;
            public IntPtr SecurityDescriptor;
            public int InheritHandle;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct StartupInfo
        {
            public int Size;
            public string Reserved;
            public string Desktop;
            public string Title;
            public uint X;
            public uint Y;
            public uint XSize;
            public uint YSize;
            public uint XCountChars;
            public uint YCountChars;
            public uint FillAttribute;
            public uint Flags;
            public short ShowWindow;
            public short Reserved2Size;
            public IntPtr Reserved2;
            public IntPtr StandardInput;
            public IntPtr StandardOutput;
            public IntPtr StandardError;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ProcessInformation
        {
            public IntPtr Process;
            public IntPtr Thread;
            public uint ProcessId;
            public uint ThreadId;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JobObjectBasicLimitInformation
        {
            public long PerProcessUserTimeLimit;
            public long PerJobUserTimeLimit;
            public uint LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public UIntPtr Affinity;
            public uint PriorityClass;
            public uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IoCounters
        {
            public ulong ReadOperationCount;
            public ulong WriteOperationCount;
            public ulong OtherOperationCount;
            public ulong ReadTransferCount;
            public ulong WriteTransferCount;
            public ulong OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JobObjectExtendedLimitInformation
        {
            public JobObjectBasicLimitInformation BasicLimitInformation;
            public IoCounters IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateJobObject(IntPtr attributes, string name);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetInformationJobObject(
            IntPtr job,
            uint informationClass,
            ref JobObjectExtendedLimitInformation information,
            uint informationLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool TerminateJobObject(IntPtr job, uint exitCode);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern bool CreateProcess(
            string applicationName,
            StringBuilder commandLine,
            IntPtr processAttributes,
            IntPtr threadAttributes,
            bool inheritHandles,
            uint creationFlags,
            IntPtr environment,
            string currentDirectory,
            ref StartupInfo startupInfo,
            out ProcessInformation processInformation);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CreatePipe(
            out IntPtr readPipe,
            out IntPtr writePipe,
            ref SecurityAttributes attributes,
            uint size);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetHandleInformation(IntPtr handle, uint mask, uint flags);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint ResumeThread(IntPtr thread);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetExitCodeProcess(IntPtr process, out uint exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool TerminateProcess(IntPtr process, uint exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr handle);

        public static AcceptanceJobResult Run(string commandLine, int timeoutMilliseconds)
        {
            if (string.IsNullOrWhiteSpace(commandLine))
            {
                throw new ArgumentException("Command line is required.", "commandLine");
            }
            if (timeoutMilliseconds < 1)
            {
                throw new ArgumentOutOfRangeException("timeoutMilliseconds");
            }

            IntPtr job = IntPtr.Zero;
            IntPtr process = IntPtr.Zero;
            IntPtr thread = IntPtr.Zero;
            IntPtr stdoutRead = IntPtr.Zero;
            IntPtr stdoutWrite = IntPtr.Zero;
            IntPtr stderrRead = IntPtr.Zero;
            IntPtr stderrWrite = IntPtr.Zero;
            StreamReader stdoutReader = null;
            StreamReader stderrReader = null;
            Task<string> stdoutTask = null;
            Task<string> stderrTask = null;
            bool assigned = false;
            bool resumed = false;

            try
            {
                job = CreateJobObject(IntPtr.Zero, null);
                if (job == IntPtr.Zero)
                {
                    throw NativeFailure("CreateJobObject");
                }
                JobObjectExtendedLimitInformation limits = new JobObjectExtendedLimitInformation();
                limits.BasicLimitInformation.LimitFlags = JobObjectLimitKillOnJobClose;
                if (!SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformationClass,
                    ref limits,
                    (uint)Marshal.SizeOf(typeof(JobObjectExtendedLimitInformation))))
                {
                    throw NativeFailure("SetInformationJobObject");
                }

                SecurityAttributes pipeAttributes = new SecurityAttributes();
                pipeAttributes.Length = Marshal.SizeOf(typeof(SecurityAttributes));
                pipeAttributes.InheritHandle = 1;
                if (!CreatePipe(out stdoutRead, out stdoutWrite, ref pipeAttributes, 0) ||
                    !SetHandleInformation(stdoutRead, HandleFlagInherit, 0) ||
                    !CreatePipe(out stderrRead, out stderrWrite, ref pipeAttributes, 0) ||
                    !SetHandleInformation(stderrRead, HandleFlagInherit, 0))
                {
                    throw NativeFailure("CreatePipe");
                }

                StartupInfo startupInfo = new StartupInfo();
                startupInfo.Size = Marshal.SizeOf(typeof(StartupInfo));
                startupInfo.Flags = StartfUseStdHandles;
                startupInfo.StandardInput = IntPtr.Zero;
                startupInfo.StandardOutput = stdoutWrite;
                startupInfo.StandardError = stderrWrite;
                ProcessInformation processInformation;
                if (!CreateProcess(
                    null,
                    new StringBuilder(commandLine),
                    IntPtr.Zero,
                    IntPtr.Zero,
                    true,
                    CreateSuspended | CreateNoWindow,
                    IntPtr.Zero,
                    Environment.CurrentDirectory,
                    ref startupInfo,
                    out processInformation))
                {
                    throw NativeFailure("CreateProcess");
                }
                process = processInformation.Process;
                thread = processInformation.Thread;

                CloseRequired(ref stdoutWrite, "stdout write pipe");
                CloseRequired(ref stderrWrite, "stderr write pipe");
                stdoutReader = CreateReader(ref stdoutRead);
                stderrReader = CreateReader(ref stderrRead);
                stdoutTask = stdoutReader.ReadToEndAsync();
                stderrTask = stderrReader.ReadToEndAsync();

                if (!AssignProcessToJobObject(job, process))
                {
                    throw NativeFailure("AssignProcessToJobObject");
                }
                assigned = true;
                if (ResumeThread(thread) == Infinite)
                {
                    throw NativeFailure("ResumeThread");
                }
                resumed = true;
                CloseRequired(ref thread, "primary thread");

                uint wait = WaitForSingleObject(process, (uint)timeoutMilliseconds);
                if (wait != WaitObject0 && wait != WaitTimeout)
                {
                    throw NativeFailure("WaitForSingleObject");
                }
                bool timedOut = wait == WaitTimeout;
                uint exitCode = 1;
                if (!timedOut && !GetExitCodeProcess(process, out exitCode))
                {
                    throw NativeFailure("GetExitCodeProcess");
                }

                TerminateAndCloseJob(ref job);
                if (timedOut)
                {
                    if (WaitForSingleObject(process, 5000) != WaitObject0)
                    {
                        throw new InvalidOperationException("Timed-out job did not terminate safely.");
                    }
                    if (!GetExitCodeProcess(process, out exitCode))
                    {
                        throw NativeFailure("GetExitCodeProcess");
                    }
                }

                string standardOutput = stdoutTask.GetAwaiter().GetResult();
                string standardError = stderrTask.GetAwaiter().GetResult();
                return new AcceptanceJobResult
                {
                    ExitCode = unchecked((int)exitCode),
                    TimedOut = timedOut,
                    StandardOutput = standardOutput,
                    StandardError = standardError
                };
            }
            finally
            {
                if (job != IntPtr.Zero)
                {
                    TerminateJobObject(job, 1);
                    CloseHandle(job);
                }
                if (process != IntPtr.Zero)
                {
                    if (!assigned || !resumed)
                    {
                        TerminateProcess(process, 1);
                        WaitForSingleObject(process, 5000);
                    }
                    CloseHandle(process);
                }
                if (thread != IntPtr.Zero)
                {
                    CloseHandle(thread);
                }
                CloseBestEffort(stdoutRead);
                CloseBestEffort(stdoutWrite);
                CloseBestEffort(stderrRead);
                CloseBestEffort(stderrWrite);
                if (stdoutReader != null)
                {
                    stdoutReader.Dispose();
                }
                if (stderrReader != null)
                {
                    stderrReader.Dispose();
                }
            }
        }

        private static StreamReader CreateReader(ref IntPtr readHandle)
        {
            SafeFileHandle safeHandle = new SafeFileHandle(readHandle, true);
            readHandle = IntPtr.Zero;
            FileStream stream = new FileStream(safeHandle, FileAccess.Read, 4096, false);
            return new StreamReader(stream, new UTF8Encoding(false), true, 4096);
        }

        private static void TerminateAndCloseJob(ref IntPtr job)
        {
            bool terminated = TerminateJobObject(job, 1);
            int terminateError = terminated ? 0 : Marshal.GetLastWin32Error();
            bool closed = CloseHandle(job);
            int closeError = closed ? 0 : Marshal.GetLastWin32Error();
            if (closed)
            {
                job = IntPtr.Zero;
            }
            if (!terminated)
            {
                throw new Win32Exception(terminateError, "TerminateJobObject failed.");
            }
            if (!closed)
            {
                throw new Win32Exception(closeError, "CloseHandle(job) failed.");
            }
        }

        private static void CloseRequired(ref IntPtr handle, string name)
        {
            if (!CloseHandle(handle))
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(), "CloseHandle(" + name + ") failed.");
            }
            handle = IntPtr.Zero;
        }

        private static void CloseBestEffort(IntPtr handle)
        {
            if (handle != IntPtr.Zero)
            {
                CloseHandle(handle);
            }
        }

        private static Win32Exception NativeFailure(string operation)
        {
            return new Win32Exception(Marshal.GetLastWin32Error(), operation + " failed.");
        }
    }
}
