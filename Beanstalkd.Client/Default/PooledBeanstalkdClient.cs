using System;
using System.Collections.Generic;

namespace Beanstalkd.Client.Default
{
    internal class BeanstalkdClientLabel
    {
        public IBeanstalkdClient Client { get; set; }
        public string CurrentTube { get; set; }
        public List<string> WatchList { get; set; }
        public DateTime LastUsage { get; set; }
    }

    internal class DisposableBeanstalkdClientLabel : IDisposable
    {
        private static DateTime _lastGC = DateTime.Now;

        private readonly BeanstalkdClientLabel _clientLabel;
        private readonly LinkedList<BeanstalkdClientLabel> _pool;

        public BeanstalkdClientLabel Client
        {
            get
            {
                return _clientLabel;
            }
        }

        public DisposableBeanstalkdClientLabel(BeanstalkdClientLabel clientLabel, LinkedList<BeanstalkdClientLabel> pool)
        {
            _clientLabel = clientLabel;
            _pool = pool;
        }

        public void Dispose()
        {
            lock (_pool)
            {
                _clientLabel.LastUsage = DateTime.Now;
                _pool.AddLast(_clientLabel);
                if (!((DateTime.Now - _lastGC).TotalSeconds > 60)) return;
                // Garbage collection: remove idle connections.
                _lastGC = DateTime.Now;
                var removeList = new List<BeanstalkdClientLabel>(_pool.Count);
                var it = _pool.First;
                while (true)
                {
                    if (it == null) break;
                    if ((DateTime.Now - it.Value.LastUsage).TotalSeconds > 120)
                    {
                        removeList.Add(it.Value);
                    }
                    if (it == _pool.Last) break;
                    it = it.Next;
                }
                removeList.ForEach(client =>
                {
                    _pool.Remove(client);
                    client.Client.Dispose();
                });
            }
        }
    }

    public class PooledBeanstalkdClient : IBeanstalkdClient
    {
        private readonly string _host;
        private readonly int _port;
        private readonly LinkedList<BeanstalkdClientLabel> _pool;

        [ThreadStatic]
        private static string _threadCurrentTube;
        [ThreadStatic]
        private static List<string> _threadWatchList;

        public PooledBeanstalkdClient(string host = "localhost", int port = 11300)
        {
            _host = host;
            _port = port;
            _pool = new LinkedList<BeanstalkdClientLabel>();
        }

        private static bool AreWatchListsEqual(IList<string> list1, IList<string> list2)
        {
            if (list1 == null && list2 == null) return true;
            if (list1 == null || list2 == null) return false;
            if (list1.Count != list2.Count) return false;
            for (var i = 0; i < list1.Count; i++)
            {
                if (list1[i] != list2[i]) return false;
            }
            return true;
        }

        private DisposableBeanstalkdClientLabel GetBeanstalkdClient()
        {
            lock (_pool)
            {
                BeanstalkdClientLabel client;
                if (_pool.Count == 0)
                {
                    client = new BeanstalkdClientLabel
                    {
                        Client = ManagedBeanstalkdClientFactory.Create(_host, _port),
                        CurrentTube = "default",
                        WatchList = new List<string> { "default" }
                    };
                }
                else
                {
                    client = _pool.First.Value;
                    _pool.RemoveFirst();
                }
                return new DisposableBeanstalkdClientLabel(client, _pool);
            }
        }

        private DisposableBeanstalkdClientLabel GetProducer(string tube)
        {
            lock (_pool)
            {
                BeanstalkdClientLabel client = null;
                if (_pool.Count == 0)
                {
                    client = new BeanstalkdClientLabel
                    {
                        Client = ManagedBeanstalkdClientFactory.Create(_host, _port),
                        CurrentTube = "default",
                        WatchList = new List<string> { "default" }
                    };
                }
                else
                {
                    var it = _pool.First;

                    while (true)
                    {
                        if (it == null) break;
                        if (it.Value.CurrentTube == tube)
                        {
                            client = it.Value;
                            break;
                        }
                        if (it == _pool.Last) break;
                        it = it.Next;
                    }
                    if (client == null)
                    {
                        client = _pool.First.Value;
                    }
                    _pool.Remove(client);
                }
                client.CurrentTube = client.Client.Use(tube);
                if (client.CurrentTube != tube)
                {
                    client.Client.Dispose();
                    throw new BeanstalkdException(BeanstalkdExceptionCode.UnexpectedResponse);
                }
                return new DisposableBeanstalkdClientLabel(client, _pool);
            }
        }

        private DisposableBeanstalkdClientLabel GetConcumer(IList<string> watchList)
        {
            lock (_pool)
            {
                BeanstalkdClientLabel client = null;
                if (_pool.Count == 0)
                {
                    client = new BeanstalkdClientLabel
                    {
                        Client = ManagedBeanstalkdClientFactory.Create(_host, _port),
                        CurrentTube = "default",
                        WatchList = new List<string> { "default" }
                    };
                }
                else
                {
                    var it = _pool.First;

                    while (true)
                    {
                        if (it == null) break;
                        if (AreWatchListsEqual(watchList, it.Value.WatchList))
                        {
                            client = it.Value;
                            break;
                        }
                        if (it == _pool.Last) break;
                        it = it.Next;
                    }
                    if (client == null)
                    {
                        client = _pool.First.Value;
                    }
                    _pool.Remove(client);
                }

                if (!AreWatchListsEqual(client.WatchList, WatchList))
                {
                    WatchList.ForEach(tube =>
                    {
                        if (!client.WatchList.Contains(tube)) client.Client.Watch(tube);
                    });
                    client.WatchList.ForEach(tube =>
                    {
                        if (!WatchList.Contains(tube)) client.Client.Ignore(tube);
                    });
                    client.WatchList = WatchList;
                }
                return new DisposableBeanstalkdClientLabel(client, _pool);
            }
        }

        public void Dispose()
        {
            Disposed = true;
            Disconnected = true;
            lock (_pool)
            {
                while (_pool.Count > 0)
                {
                    _pool.First.Value.Client.Dispose();
                    _pool.RemoveFirst();
                }
            }
        }

        public bool Disconnected { get; private set; }
        public bool Disposed { get; private set; }

        public string CurrentTube
        {
            get
            {
                if (string.IsNullOrEmpty(_threadCurrentTube)) _threadCurrentTube = "default";
                return _threadCurrentTube;
            }
        }

        public string Use(string tube)
        {
            _threadCurrentTube = tube;
            return _threadCurrentTube;
        }

        public uint Put(byte[] data, uint priority = 4294967295, uint delay = 0, uint ttr = 10)
        {
            if (string.IsNullOrEmpty(_threadCurrentTube)) _threadCurrentTube = "default";
            return Put(_threadCurrentTube, data, priority, delay, ttr);
        }

        public uint Put(string tube, byte[] data, uint priority = 4294967295, uint delay = 0, uint ttr = 10)
        {
            if (string.IsNullOrEmpty(tube)) throw new ArgumentNullException("tube");
            using (var wrapper = GetProducer(tube))
            {
                return wrapper.Client.Client.Put(data, priority, delay, ttr);
            }
        }

        public List<string> WatchList
        {
            get
            {
                if (_threadWatchList == null || _threadWatchList.Count == 0) _threadWatchList = new List<string> { "default" };
                return new List<string>(_threadWatchList);
            }
        }

        public uint Watch(string tube)
        {
            if (!WatchList.Contains(tube))
            {
                WatchList.Add(tube);
                WatchList.Sort();
            }
            return (uint)WatchList.Count;
        }

        public bool Ignore(string tube)
        {
            return WatchList.Remove(tube);
        }

        public ReserveStatus Reserve(out Job job)
        {
            using (var wrapper = GetConcumer(WatchList))
            {
                return wrapper.Client.Client.Reserve(out job);
            }
        }

        public ReserveStatus Reserve(uint timeout, out Job job)
        {
            using (var wrapper = GetConcumer(WatchList))
            {
                return wrapper.Client.Client.Reserve(timeout, out job);
            }
        }

        public bool Delete(uint jobId)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Delete(jobId);
            }
        }

        public bool Release(uint jobId, uint priority = 4294967295, uint delay = 0)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Release(jobId, priority, delay);
            }
        }

        public bool Bury(uint jobId, uint priority = 4294967295)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Bury(jobId, priority);
            }
        }

        public bool Touch(uint jobId)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Touch(jobId);
            }
        }

        public Job Peek(uint jobId)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Peek(jobId);
            }
        }

        public Job PeekReady()
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.PeekReady();
            }
        }

        public Job PeekBuried()
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.PeekBuried();
            }
        }

        public Job PeekDelayed()
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.PeekDelayed();
            }
        }

        public uint Kick(uint bound)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Kick(bound);
            }
        }

        public JobStats StatsJob(uint jobId)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.StatsJob(jobId);
            }
        }

        public TubeStats StatsTube(string tube)
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.StatsTube(tube);
            }
        }

        public ServerStats Stats()
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.Stats();
            }
        }

        public List<string> ListTubes()
        {
            using (var wrapper = GetBeanstalkdClient())
            {
                return wrapper.Client.Client.ListTubes();
            }
        }
    }
}
