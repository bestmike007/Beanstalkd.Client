using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Beanstalkd.Client.Default
{
    internal enum ExpectType
    {
        ResponseLine,
        Data,
        Nothing
    }

    internal class ReadState
    {
        public ExpectType ExpectType { get; set; }
        public int DataLength { get; set; }
        public byte LastByte { get; set; }
        public Command Command { get; set; }
    }

    public class BeanstalkdClient : IBeanstalkdClient
    {
        protected TcpClient TcpClient;
        private readonly Queue<Command> _queue;
        private MemoryStream _receiveBuff;
        private readonly byte[] _byteBuff = new byte[256];
        private readonly ReadState _readState;

        public BeanstalkdClient(string host = "localhost", int port = 11300)
        {
            var connectionTimeout = new ManualResetEvent(false);
            TcpClient = new TcpClient();

            TcpClient.BeginConnect(host, port, delegate(IAsyncResult result)
            {
                try
                {
                    if (TcpClient.Client != null)
                        TcpClient.EndConnect(result);
                    else
                    {
                        TcpClient.Close();
                        TcpClient = null;
                    }
                }
                catch (Exception ex)
                {
                    LogProvider.Current.Error(ex, "Error connecting to server.");
                    if (TcpClient != null)
                    {
                        TcpClient.Close();
                        TcpClient = null;
                    }
                }
                connectionTimeout.Set();
            }, TcpClient);

            if (connectionTimeout.WaitOne(10000, false) && TcpClient != null)
            {
                var recBuff = new byte[20];
                TcpClient.Client.Send(new byte[] { 65, 13, 10 });
                TcpClient.Client.Receive(recBuff);

                if (Encoding.ASCII.GetString(recBuff, 0, 17) != "UNKNOWN_COMMAND\r\n")
                    throw new BeanstalkdException(BeanstalkdExceptionCode.ConnectionError);
            }
            else
            {
                throw new BeanstalkdException(BeanstalkdExceptionCode.ConnectionError);
            }

            _readState = new ReadState { ExpectType = ExpectType.Nothing };
            _queue = new Queue<Command>();
            _receiveBuff = new MemoryStream(8192);
            TcpClient.Client.BeginReceive(_byteBuff, 0, _byteBuff.Length, SocketFlags.None, OnReceive, TcpClient.Client);
        }

        private void OnReceive(IAsyncResult ar)
        {
            var socket = (Socket)ar.AsyncState;
            if (!socket.Connected) return;
            SocketError socketError;
            var size = socket.EndReceive(ar, out socketError);
            if (size == 0 || socketError != SocketError.Success)
            {
                if (size > 0) LogProvider.Current.InfoFormat("Socket disconnected, code: {0}.", socketError);
                else LogProvider.Current.InfoFormat("Server socket closed.");
                Dispose();
                return;
            }
            lock (_readState) for (var i = 0; i < size; i++) OnBuffer(_byteBuff[i]);

            TcpClient.Client.BeginReceive(_byteBuff, 0, _byteBuff.Length, SocketFlags.None, out socketError, OnReceive,
                TcpClient.Client);
            if (socketError == SocketError.Success) return;
            LogProvider.Current.WarnFormat("Fail to begin receive from socket, code: {0}.", socketError);
            Dispose();
        }

        private void OnBuffer(byte b)
        {
            switch (_readState.ExpectType)
            {
                case ExpectType.ResponseLine:
                    _receiveBuff.WriteByte(b);
                    if (_receiveBuff.Length == 0)
                    {
                        _readState.LastByte = 0;
                        return;
                    }
                    if (_readState.LastByte == '\r' && b == '\n')
                    {
                        var buffer = _receiveBuff.ToArray();
                        _readState.Command.ResponseLine = Encoding.ASCII.GetString(buffer).Trim();
                        _receiveBuff = new MemoryStream(8192);
                        if (_readState.Command.ExpectData != null &&
                            _readState.Command.ExpectData(_readState.Command.ResponseLine))
                        {
                            int dataLength;
                            if (
                                !int.TryParse(
                                    _readState.Command.ResponseLine.Substring(
                                        _readState.Command.ResponseLine.LastIndexOf(' ') + 1), out dataLength))
                            {
                                _readState.Command.Code = BeanstalkdExceptionCode.UnexpectedResponse;
                                Dispose();
                                return;
                            }
                            _readState.LastByte = 0;
                            _readState.DataLength = dataLength;
                            _readState.ExpectType = ExpectType.Data;
                        }
                        else EndCommand();
                    }
                    _readState.LastByte = b;
                    break;
                case ExpectType.Data:
                    _receiveBuff.WriteByte(b);
                    if (_receiveBuff.Length >= _readState.DataLength + 2)
                    {
                        var buffer = _receiveBuff.ToArray();
                        if (buffer[_readState.DataLength] != '\r' || buffer[_readState.DataLength + 1] != '\n')
                        {
                            _readState.Command.Code = BeanstalkdExceptionCode.UnexpectedResponse;
                            Dispose();
                            return;
                        }
                        _readState.Command.ResponseData = new byte[_readState.DataLength];
                        Array.Copy(buffer, _readState.Command.ResponseData, _readState.DataLength);
                        _receiveBuff = new MemoryStream(8192);
                        EndCommand();
                    }
                    break;
            }
        }

        private void BeginCommand(Command command = null)
        {
            if (command != null) LogProvider.Current.DebugFormat("Begin send command {0}", command.RequestLine);
            if (Disconnected || Disposed)
            {
                if (command != null)
                {
                    command.Code = BeanstalkdExceptionCode.ConnectionError;
                    command.WaitHandle.Set();
                }
                return;
            }
            if (command != null)
            {
                lock (_queue) _queue.Enqueue(command);
            }
            lock (_readState)
            {
                if (_readState.Command != null) return;
                lock (_queue)
                {
                    if (_queue.Count == 0) return;
                    command = _queue.Dequeue();
                }
                _readState.Command = command;
                _readState.DataLength = 0;
                _readState.ExpectType = ExpectType.ResponseLine;
                _readState.LastByte = 0;
                var buffer = new List<byte>();
                buffer.AddRange(Encoding.ASCII.GetBytes(command.RequestLine + "\r\n"));
                if (command.RequestData != null)
                {
                    buffer.AddRange(command.RequestData);
                    buffer.AddRange(Encoding.ASCII.GetBytes("\r\n"));
                }
                SocketError error;
                TcpClient.Client.Send(buffer.ToArray(), 0, buffer.Count, SocketFlags.None, out error);
                if (error == SocketError.Success) return;
                LogProvider.Current.WarnFormat("Unable to send {0}, error: {1}.", command.RequestLine, error);
                command.Code = BeanstalkdExceptionCode.ConnectionError;
                Dispose();
            }
        }

        private void EndCommand()
        {
            lock (_readState)
            {
                var command = _readState.Command;
                _readState.Command = null;
                command.WaitHandle.Set();
                LogProvider.Current.DebugFormat("End command (request: {0}, response: {1})", command.RequestLine, command.ResponseLine);
                if (!Disconnected && !Disposed) BeginCommand();
            }
        }

        public void Dispose()
        {
            try
            {
                lock (_readState)
                {
                    if (_readState.Command != null)
                    {
                        if (_readState.Command.Code == null)
                            _readState.Command.Code = BeanstalkdExceptionCode.ConnectionError;
                        _readState.Command.WaitHandle.Set();
                    }
                }
                lock (_queue)
                {
                    while (_queue.Count > 0)
                    {
                        var command = _queue.Dequeue();
                        if (command.Code == null) command.Code = BeanstalkdExceptionCode.ConnectionError;
                        command.WaitHandle.Set();
                    }
                }
                if (TcpClient == null) return;
                if (TcpClient.Connected) TcpClient.Close();
                TcpClient = null;
            }
            finally
            {
                Disposed = true;
            }
        }

        public bool Disconnected
        {
            get
            {
                try
                {
                    return Disposed || !TcpClient.Connected;
                }
                catch (Exception)
                {
                    Dispose();
                    return true;
                }
            }
        }

        public bool Disposed { get; private set; }

        public string CurrentTube
        {
            get
            {
                var command = new Command { RequestLine = "list-tube-used" };
                BeginCommand(command);
                command.Wait();
                var match = Regex.Match(command.ResponseLine, "^USING (.+)$");
                if (!match.Success) throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
                return match.Groups[1].Value;
            }
        }

        public string Use(string tube)
        {
            if (string.IsNullOrEmpty(tube)) throw new ArgumentNullException("tube");
            var command = new Command { RequestLine = string.Format("use {0}", tube) };
            BeginCommand(command);
            command.Wait();

            var match = Regex.Match(command.ResponseLine, "^USING (.+)$");
            if (!match.Success) throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return match.Groups[1].Value;
        }

        public uint Put(byte[] data, uint priority = 4294967295, uint delay = 0, uint ttr = 10)
        {
            if (data == null) throw new ArgumentNullException("data");
            if (data.Length > 65536) throw new BeanstalkdException(BeanstalkdExceptionCode.JobTooBig);
            var command = new Command
            {
                RequestData = data,
                RequestLine = string.Format("put {0} {1} {2} {3}", priority, delay, ttr, data.Length)
            };
            BeginCommand(command);
            command.Wait();

            if (command.ResponseLine.Equals("JOB_TOO_BIG", StringComparison.OrdinalIgnoreCase))
                throw new BeanstalkdException(BeanstalkdExceptionCode.JobTooBig);
            uint id;
            var match = Regex.Match(command.ResponseLine, "^(INSERTED|BURIED) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[2].Value, out id))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return id;
        }

        public uint Put(string tube, byte[] data, uint priority = 4294967295, uint delay = 0, uint ttr = 10)
        {
            Use(tube);
            return Put(data, priority, delay, ttr);
        }

        public List<string> WatchList
        {
            get
            {
                var command = new Command
                {
                    RequestLine = "list-tubes-watched",
                    ExpectData = line => Regex.IsMatch(line, "^OK \\d+$")
                };
                BeginCommand(command);
                command.Wait();

                try
                {
                    return new List<string>(LoadYaml(command.ResponseData).Keys);
                }
                catch (Exception)
                {
                    throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
                }
            }
        }

        public ReserveStatus Reserve(out Job job)
        {
            var command = new Command
            {
                RequestLine = "reserve",
                ExpectData = line => Regex.IsMatch(line, "^RESERVED (\\d+) (\\d+)$")
            };
            BeginCommand(command);
            command.Wait();

            if ("DEADLINE_SOON".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase))
            {
                job = null;
                return ReserveStatus.DeadlineSoon;
            }

            if ("TIMED_OUT".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase))
            {
                job = null;
                return ReserveStatus.Timeout;
            }
            uint id, size;
            var match = Regex.Match(command.ResponseLine, "^RESERVED (\\d+) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out id) ||
                !uint.TryParse(match.Groups[2].Value, out size))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            job = new Job(id, command.ResponseData);
            return ReserveStatus.Reserved;
        }

        public ReserveStatus Reserve(uint timeout, out Job job)
        {
            var command = new Command
            {
                RequestLine = string.Format("reserve-with-timeout {0}", timeout),
                ExpectData = line => Regex.IsMatch(line, "^RESERVED (\\d+) (\\d+)$")
            };
            BeginCommand(command);
            command.Wait();

            if ("DEADLINE_SOON".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase))
            {
                job = null;
                return ReserveStatus.DeadlineSoon;
            }

            if ("TIMED_OUT".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase))
            {
                job = null;
                return ReserveStatus.Timeout;
            }
            uint id, size;
            var match = Regex.Match(command.ResponseLine, "^RESERVED (\\d+) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out id) ||
                !uint.TryParse(match.Groups[2].Value, out size))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            job = new Job(id, command.ResponseData);
            return ReserveStatus.Reserved;
        }

        public bool Delete(uint jobId)
        {
            var command = new Command { RequestLine = string.Format("delete {0}", jobId) };
            BeginCommand(command);
            command.Wait();

            return "DELETED".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase);
        }

        public bool Release(uint jobId, uint priority = 4294967295, uint delay = 0)
        {
            var command = new Command { RequestLine = string.Format("release {0} {1} {2}", jobId, priority, delay) };
            BeginCommand(command);
            command.Wait();

            return "RELEASED".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase) ||
                   "BURIED".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase);
        }

        public bool Bury(uint jobId, uint priority = 4294967295)
        {
            var command = new Command { RequestLine = string.Format("bury {0} {1}", jobId, priority) };
            BeginCommand(command);
            command.Wait();

            return "BURIED".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase);
        }

        public bool Touch(uint jobId)
        {
            var command = new Command { RequestLine = string.Format("touch {0}", jobId) };
            BeginCommand(command);
            command.Wait();

            return "TOUCHED".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase);
        }

        public uint Watch(string tube)
        {
            if (string.IsNullOrEmpty(tube)) throw new ArgumentNullException("tube");
            var command = new Command { RequestLine = string.Format("watch {0}", tube) };
            BeginCommand(command);
            command.Wait();

            uint count;
            var match = Regex.Match(command.ResponseLine, "^WATCHING (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out count))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return count;
        }

        public bool Ignore(string tube)
        {
            if (string.IsNullOrEmpty(tube)) throw new ArgumentNullException("tube");
            var command = new Command { RequestLine = string.Format("ignore {0}", tube) };
            BeginCommand(command);
            command.Wait();

            return !"NOT_IGNORED".Equals(command.ResponseLine) &&
                   Regex.IsMatch(command.ResponseLine, "^WATCHING (\\d+)$");
        }

        public Job Peek(uint jobId)
        {
            var command = new Command
            {
                RequestLine = string.Format("peek {0}", jobId),
                ExpectData = line => Regex.IsMatch(line, "^FOUND (\\d+) (\\d+)$")
            };
            BeginCommand(command);
            command.Wait();

            if ("NOT_FOUND".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase)) return null;
            uint id;
            var match = Regex.Match(command.ResponseLine, "^FOUND (\\d+) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out id))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return new Job(id, command.ResponseData);
        }

        public Job PeekReady()
        {
            var command = new Command
            {
                RequestLine = "peek-ready",
                ExpectData = line => Regex.IsMatch(line, "^FOUND (\\d+) (\\d+)$")
            };
            BeginCommand(command);
            command.Wait();

            if ("NOT_FOUND".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase)) return null;
            uint id;
            var match = Regex.Match(command.ResponseLine, "^FOUND (\\d+) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out id))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return new Job(id, command.ResponseData);
        }

        public Job PeekBuried()
        {
            var command = new Command
            {
                RequestLine = "peek-buried",
                ExpectData = line => Regex.IsMatch(line, "^FOUND (\\d+) (\\d+)$")
            };
            BeginCommand(command);
            command.Wait();

            if ("NOT_FOUND".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase)) return null;
            uint id;
            var match = Regex.Match(command.ResponseLine, "^FOUND (\\d+) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out id))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return new Job(id, command.ResponseData);
        }

        public Job PeekDelayed()
        {
            var command = new Command
            {
                RequestLine = "peek-delayed",
                ExpectData = line => Regex.IsMatch(line, "^FOUND (\\d+) (\\d+)$")
            };
            BeginCommand(command);
            command.Wait();

            if ("NOT_FOUND".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase)) return null;
            uint id;
            var match = Regex.Match(command.ResponseLine, "^FOUND (\\d+) (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out id))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return new Job(id, command.ResponseData);
        }

        public uint Kick(uint bound)
        {
            var command = new Command { RequestLine = string.Format("kick {0}", bound) };
            BeginCommand(command);
            command.Wait();

            uint count;
            var match = Regex.Match(command.ResponseLine, "^KICKED (\\d+)$");
            if (!match.Success || !uint.TryParse(match.Groups[1].Value, out count))
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            return count;
        }

        public JobStats StatsJob(uint jobId)
        {
            var command = new Command
            {
                RequestLine = string.Format("stats-job {0}", jobId),
                ExpectData = line => Regex.IsMatch(line, "^OK \\d+$")
            };
            BeginCommand(command);
            command.Wait();

            if ("NOT_FOUND".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase)) return null;
            try
            {
                return JobStats.Parse(LoadYaml(command.ResponseData));
            }
            catch (Exception)
            {
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            }
        }

        public TubeStats StatsTube(string tube)
        {
            if (string.IsNullOrEmpty(tube)) throw new ArgumentNullException("tube");
            var command = new Command
            {
                RequestLine = string.Format("stats-tube {0}", tube),
                ExpectData = line => Regex.IsMatch(line, "^OK \\d+$")
            };
            BeginCommand(command);
            command.Wait();

            if ("NOT_FOUND".Equals(command.ResponseLine, StringComparison.OrdinalIgnoreCase)) return null;
            try
            {
                return TubeStats.Parse(LoadYaml(command.ResponseData));
            }
            catch (Exception)
            {
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            }
        }

        public ServerStats Stats()
        {
            var command = new Command { RequestLine = "stats", ExpectData = line => Regex.IsMatch(line, "^OK \\d+$") };
            BeginCommand(command);
            command.Wait();

            try
            {
                return ServerStats.Parse(LoadYaml(command.ResponseData));
            }
            catch (Exception)
            {
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            }
        }

        public List<string> ListTubes()
        {
            var command = new Command
            {
                RequestLine = "list-tubes",
                ExpectData = line => Regex.IsMatch(line, "^OK \\d+$")
            };
            BeginCommand(command);
            command.Wait();

            try
            {
                return new List<string>(LoadYaml(command.ResponseData).Keys);
            }
            catch (Exception)
            {
                throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
            }
        }

        private static Dictionary<string, string> LoadYaml(byte[] data)
        {
            var response = Encoding.ASCII.GetString(data);

            response = response.Trim('\r', '\n');

            var yaml = new Dictionary<string, string>();

            using (var sr = new StringReader(response))
            {
                while (true)
                {
                    var line = sr.ReadLine();

                    if (line == null)
                        break;

                    if (line.Substring(0, 2).Equals("- "))
                    {
                        yaml.Add(line.Substring(2), null);
                        continue;
                    }

                    var lineParts = line.Split(new[] { ':' }, 2);

                    if (lineParts.Length == 2)
                        yaml.Add(lineParts[0], lineParts[1].Trim());
                }
            }
            return yaml;
        }
    }

    internal class Command
    {
        public Command()
        {
            WaitHandle = new ManualResetEvent(false);
        }

        public void Wait()
        {
            WaitHandle.WaitOne();
            if (Code != null) throw new BeanstalkdException(Code.Value);
        }

        public EventWaitHandle WaitHandle { get; private set; }
        public ExpectData ExpectData { get; set; }
        public string RequestLine { get; set; }
        public byte[] RequestData { get; set; }
        public string ResponseLine { get; set; }
        public byte[] ResponseData { get; set; }
        public BeanstalkdExceptionCode? Code { get; set; }
    }

    internal delegate bool ExpectData(string responseLine);
}