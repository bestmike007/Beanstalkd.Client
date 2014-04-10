using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

namespace Beanstalkd.Client.Default
{
    public class JobBulkProducer : IBulkProducer
    {
        private readonly string _host;
        private readonly int _port;

        public JobBulkProducer(string host, int port = 11300)
        {
            _host = host;
            _port = port;
        }

        public List<uint?> Put(string tube, List<byte[]> data, uint priority = 4294967295, uint delay = 0, uint ttr = 10)
        {
            if (data == null || data.Count == 0) return new List<uint?>();
            if (string.IsNullOrEmpty(tube)) tube = "default";
            try
            {
                using (var client = new TcpClient())
                {
                    client.Connect(_host, _port);
                    using (var stream = client.GetStream())
                    using (var reader = new StreamReader(stream, new ASCIIEncoding()))
                    using (var writer = new StreamWriter(stream, new ASCIIEncoding()))
                    {
                        // test connection
                        writer.Write("use {0}\r\n", tube);
                        writer.Flush();
                        var line = reader.ReadLine();
                        if (line != string.Format("USING {0}", tube)) throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
                        // bulk put
                        foreach (var item in data)
                        {
                            writer.Write("put {0} {1} {2} {3}\r\n", priority, delay, ttr, item.Length);
                            writer.Flush();
                            stream.Write(item, 0, item.Length);
                            writer.Write("\r\n");
                        }
                        writer.Flush();
                        // get results
                        var result = new List<uint?>(data.Count);
                        while (result.Count < data.Count)
                        {
                            line = reader.ReadLine();
                            if (string.IsNullOrEmpty(line)) throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
                            if (line.Equals("JOB_TOO_BIG", StringComparison.OrdinalIgnoreCase))
                            {
                                result.Add(null);
                                continue;
                            }
                            uint id;
                            var match = Regex.Match(line, "^(INSERTED|BURIED) (\\d+)$");
                            if (match.Success && uint.TryParse(match.Groups[2].Value, out id))
                            {
                                result.Add(id);
                                continue;
                            }
                            if (line.Equals("DRAINING", StringComparison.OrdinalIgnoreCase))
                            {
                                result.Add(null);
                                continue;
                            }
                            throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
                        }
                        return result;
                    }
                }
            }
            catch (Exception ex)
            {
                LogProvider.Current.FatalFormat(ex, "Unexpected error when performing bulk put jobs.");
                throw new BeanstalkdException(BeanstalkdExceptionCode.ConnectionError);
            }
        }
    }
}
