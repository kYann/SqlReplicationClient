using Moq;
using SqlReplicationClient.Servers;
using SqlReplicationClient.Sessions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SqlReplicationClient.Tests
{
    public class DefaultConnectionManagerTests
    {
        private IConnectionManager CreateCnxManager(Session session,
            IEnumerable<Server> activeServers,
            IEnumerable<Server> failoverServers)
        {
            var sessionManager = new Mock<ISessionManager>(MockBehavior.Loose);
            return this.CreateCnxManager(session, sessionManager, activeServers, failoverServers);
        }

        private IConnectionManager CreateCnxManager(Session session,
            Mock<ISessionManager> sessionManager,
            IEnumerable<Server> activeServers,
            IEnumerable<Server> failoverServers)
        {
            sessionManager.Setup(c => c.GetCurrentSession()).Returns(session);
            var cnx = new Mock<IDbConnection>(MockBehavior.Loose);
            cnx.SetupProperty(c => c.ConnectionString);

            var cnxManager = new DefaultConnectionManager(sessionManager.Object,
                () => cnx.Object,
                activeServers, failoverServers);
            return cnxManager;
        }

        [Fact]
        public void WhenAskForWritableConnection_ThenSessionGoesWritable()
        {
            var session = new Session();
            var sessionManager = new Mock<ISessionManager>(MockBehavior.Loose);
            sessionManager.Setup(c => c.SetSessionOnWritable()).Verifiable();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = true, IsWritable = true },
            };
            var cnxManager = this.CreateCnxManager(session, sessionManager,
                activeServers, Enumerable.Empty<Server>());

            Assert.False(session.IsInWritableMode());

            var dbCnx = cnxManager.GetWritableConnection();

            sessionManager.Verify(c => c.SetSessionOnWritable(), Times.Once(), "set session write not called");
        }

        [Fact]
        public void WhenAskForReadableConnectionAndSessionWasInWriteMode_ThenGetWritableConnection()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = true, IsWritable = true },
                new Server("active2"){ IsUp = false, IsWritable = false },
                new Server("active2"){ IsUp = false, IsWritable = false },
                new Server("active2"){ IsUp = false, IsWritable = false },
                new Server("active2"){ IsUp = false, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session,
                activeServers, Enumerable.Empty<Server>());

            session.SetWritableMode();

            var dbCnx = cnxManager.GetReadableConnection();

            Assert.Equal("active1", dbCnx.ConnectionString);
        }

        [Fact]
        public void WhenAskForWritableConnection_ThenGiveWritableConnection()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = true, IsWritable = true },
                new Server("active2"){ IsUp = false, IsWritable = false },
                new Server("active2"){ IsUp = false, IsWritable = false },
                new Server("active2"){ IsUp = false, IsWritable = false },
                new Server("active2"){ IsUp = false, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session,
                activeServers, Enumerable.Empty<Server>());

            var dbCnx = cnxManager.GetWritableConnection();

            Assert.Equal("active1", dbCnx.ConnectionString);
        }

        [Fact]
        public void WhenAskForReadableConnectionAndNoUpConnectionIsAvailable_ThenGiveFailOverConnection()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = false, IsWritable = true },
                new Server("active2"){ IsUp = false, IsWritable = false },
            };
            var failoverServers = new Server[]{
                new Server("failover"){ IsUp = true, IsWritable = true },
                new Server("failover"){ IsUp = true, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session, activeServers, failoverServers);

            var dbCnx = cnxManager.GetReadableConnection();

            Assert.Equal("failover", dbCnx.ConnectionString);
        }

        [Fact]
        public void WhenAskForWritableConnectionAndNoUpConnectionIsAvailable_ThenGiveFailOverConnection()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = false, IsWritable = true },
                new Server("active2"){ IsUp = false, IsWritable = false },
            };
            var failoverServers = new Server[]{
                new Server("failover"){ IsUp = true, IsWritable = true },
                new Server("failover"){ IsUp = true, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session, activeServers, failoverServers);

            var dbCnx = cnxManager.GetWritableConnection();

            Assert.Equal("failover", dbCnx.ConnectionString);
        }

        [Fact]
        public void WhenAskForWritableConnectionAndNoUpConnectionIsAvailable_ThenThrowException()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = false, IsWritable = true },
                new Server("active2"){ IsUp = false, IsWritable = false },
            };
            var failoverServers = new Server[]{
                new Server("failover"){ IsUp = false, IsWritable = true },
                new Server("failover"){ IsUp = false, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session, activeServers, failoverServers);

            Assert.Throws<Exception>(() =>
                {
                    var dbCnx = cnxManager.GetWritableConnection();
                });
        }

        [Fact]
        public void WhenAskForReadableConnectionAndNoUpConnectionIsAvailable_ThenThrowException()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = false, IsWritable = true },
                new Server("active2"){ IsUp = false, IsWritable = false },
            };
            var failoverServers = new Server[]{
                new Server("failover"){ IsUp = false, IsWritable = true },
                new Server("failover"){ IsUp = false, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session, activeServers, failoverServers);

            Assert.Throws<Exception>(() =>
            {
                var dbCnx = cnxManager.GetWritableConnection();
            });
        }

        [Fact]
        public void WhenAskForReadableConnectionAndNoNotDelayConnectionIsAvailable_ThenGiveFailOverConnection()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = true, IsWritable = true, DelayFromMaster = TimeSpan.FromMinutes(1)  },
                new Server("active2"){ IsUp = true, IsWritable = false, DelayFromMaster = TimeSpan.FromMinutes(1) },
            };
            var failoverServers = new Server[]{
                new Server("failover"){ IsUp = true, IsWritable = true },
                new Server("failover"){ IsUp = true, IsWritable = false },
            };
            var cnxManager = this.CreateCnxManager(session, activeServers, failoverServers);

            var dbCnx = cnxManager.GetReadableConnection();

            Assert.Equal("failover", dbCnx.ConnectionString);
        }

        [Fact]
        public void WhenAskForReadableConnectionAndNoNotDelayConnectionIsAvailable_ThenThrow()
        {
            var session = new Session();
            var activeServers = new Server[]{
                new Server("active1"){ IsUp = true, IsWritable = true, DelayFromMaster = TimeSpan.FromMinutes(1) },
                new Server("active2"){ IsUp = true, IsWritable = false, DelayFromMaster = TimeSpan.FromMinutes(1) },
            };
            var failoverServers = new Server[]{
                new Server("failover"){ IsUp = true, IsWritable = true, DelayFromMaster = TimeSpan.FromMinutes(1) },
                new Server("failover"){ IsUp = true, IsWritable = false, DelayFromMaster = TimeSpan.FromMinutes(1) },
            };
            var cnxManager = this.CreateCnxManager(session, activeServers, failoverServers);

            Assert.Throws<Exception>(() =>
            {
                var dbCnx = cnxManager.GetReadableConnection();
            });
        }
    }
}
