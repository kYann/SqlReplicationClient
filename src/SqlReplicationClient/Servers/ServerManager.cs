using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;

namespace SqlReplicationClient.Servers
{
    public class ServerManager
    {
        readonly ServerChecker serverChecker;
        IEnumerable<Server> activeServers;
        IEnumerable<Server> failoverServers;

        static Dictionary<ServerManager, Timer> checkerTimers = new Dictionary<ServerManager,Timer>();

        public ServerManager(ServerChecker serverChecker,
            IEnumerable<string> connectionStrings,
            IEnumerable<string> failoverConnectionStrings)
        {
            this.serverChecker = serverChecker;
            this.activeServers = connectionStrings
                .Select(c => new Server(c)).ToList();
            this.failoverServers = failoverConnectionStrings
                .Select(c => new Server(c)).ToList();
        }

        public ServerManager(ServerChecker serverChecker,
            IEnumerable<string> connectionStrings)
            : this(serverChecker, connectionStrings,
            Enumerable.Empty<string>())
        {
        }

        public IEnumerable<Server> GetActiveServers()
        {
            return this.activeServers;
        }

        public IEnumerable<Server> GetFailoverServers()
        {
            return this.failoverServers;
        }

        public static ServerManager Start(ServerChecker serverChecker,
            IEnumerable<string> connectionStrings,
            int checkInterval = 1000)
        {
            return Start(serverChecker, connectionStrings,
                Enumerable.Empty<string>(), checkInterval);
        }

        public static ServerManager Start(ServerChecker serverChecker,
            IEnumerable<string> connectionStrings,
            IEnumerable<string> failoverConnectionStrings,
            int checkInterval = 1000)
        {
            var sm = new ServerManager(serverChecker, connectionStrings, failoverConnectionStrings);
            var checkerTimer = new Timer(checkInterval);

            sm.serverChecker.DoCheckupOnAllServers(
                        sm.activeServers.Union(sm.failoverServers));

            checkerTimer.Elapsed += (o, e) =>
                {
                    checkerTimer.Enabled = false;
                    try
                    {
                        sm.serverChecker.DoCheckupOnAllServers(
                            sm.activeServers.Union(sm.failoverServers));
                    }
                    finally
                    {
                        checkerTimer.Enabled = true;
                    }
                };
            checkerTimer.Start();
            checkerTimers.Add(sm, checkerTimer);

            return sm;
        }

        public static void Stop(ServerManager serverManager)
        {
            if (checkerTimers.ContainsKey(serverManager))
                checkerTimers[serverManager].Stop();
        }

        public static void StopAll()
        {
            foreach (var item in checkerTimers.Values)
            {
                item.Stop();
            }
        }
    }
}
