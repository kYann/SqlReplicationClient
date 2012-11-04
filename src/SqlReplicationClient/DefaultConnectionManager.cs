using SqlReplicationClient.Servers;
using SqlReplicationClient.Sessions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient
{
    public class DefaultConnectionManager : IConnectionManager
    {
        readonly Func<IDbConnection> connectionFactory;
        readonly IEnumerable<Server> activeServers;
        readonly IEnumerable<Server> failoverServers;
        readonly ISessionManager sessionManager;
        readonly int maxReplicationDelay = 2000;

        public DefaultConnectionManager(ISessionManager sessionManager,
            Func<IDbConnection> connectionFactory,
            IEnumerable<Server> activeServers,
            int maxReplicationDelay = 2000)
            : this(sessionManager, connectionFactory, activeServers,
            Enumerable.Empty<Server>(), maxReplicationDelay)
        {
        }

        public DefaultConnectionManager(Func<IDbConnection> connectionFactory,
            IEnumerable<Server> activeServers,
            int maxReplicationDelay = 2000)
            : this(SessionManager.Current,
            connectionFactory, activeServers, maxReplicationDelay)
        {
        }

        public DefaultConnectionManager(Func<IDbConnection> connectionFactory,
            IEnumerable<Server> activeServers,
            IEnumerable<Server> failoverServers,
            int maxReplicationDelay = 2000)
            : this(SessionManager.Current,
            connectionFactory, activeServers, failoverServers, 
            maxReplicationDelay)
        {
        }

        public DefaultConnectionManager(ISessionManager sessionManager,
            Func<IDbConnection> connectionFactory,
            IEnumerable<Server> activeServers,
            IEnumerable<Server> failoverServers,
            int maxReplicationDelay = 2000)
        {
            this.sessionManager = sessionManager;
            this.connectionFactory = connectionFactory;
            this.activeServers = activeServers;
            this.failoverServers = failoverServers;
            this.maxReplicationDelay = maxReplicationDelay;
        }

        protected virtual Server BalanceServers(IEnumerable<Server> servers)
        {
            var rand = new Random();
            return servers.OrderBy(c => rand.Next()).FirstOrDefault();
        }

        protected virtual IDbConnection CreateConnection(string connectionString)
        {
            var cnx = connectionFactory();
            cnx.ConnectionString = connectionString;
            return cnx;
        }

        protected virtual Server SelectWritableServer(IEnumerable<Server> servers)
        {
            var eligibleServers = servers.Where(c => c.IsWritable && c.IsUp);
            return this.BalanceServers(eligibleServers);
        }

        protected virtual Server SelectAvailableServer(IEnumerable<Server> servers)
        {
            var eligibleServers = servers.Where(c => c.IsUp)
                .Where(c => c.DelayFromMaster.TotalMilliseconds < maxReplicationDelay);
            return  this.BalanceServers(eligibleServers);
        }

        public virtual IDbConnection GetWritableConnection()
        {
            this.sessionManager.SetSessionOnWritable();
            var activeWritableServer = this.SelectWritableServer(this.activeServers);
            if (activeWritableServer == null)
                activeWritableServer = this.SelectWritableServer(this.failoverServers);
            if (activeWritableServer == null)
                throw new Exception("No writable instance found");

            return CreateConnection(activeWritableServer.ConnectionString);
        }

        public virtual IDbConnection GetReadableConnection()
        {
            bool nop;
            return this.GetReadableConnection(out nop);
        }

        public virtual IDbConnection GetReadableConnection(out bool isWritableConnection)
        {
            isWritableConnection = false;
            if (this.sessionManager.GetCurrentSession().IsInWritableMode())
            {
                isWritableConnection = true;
                return this.GetWritableConnection();
            }

            var activeServer = this.SelectAvailableServer(this.activeServers);
            if (activeServer == null)
                activeServer = this.SelectAvailableServer(this.failoverServers);
            if (activeServer == null)
                throw new Exception("No available instance found");

            return CreateConnection(activeServer.ConnectionString);
        }

    }
}
