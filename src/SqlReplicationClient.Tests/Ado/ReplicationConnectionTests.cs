using Moq;
using SqlReplicationClient.Ado;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SqlReplicationClient.Tests.Ado
{
    public class ReplicationConnectionTests
    {
        private IConnectionManager CreateConnectionManager()
        {
            var cnxMaster = new Mock<IDbConnection>();
            cnxMaster.Setup(c => c.ConnectionString).Returns("master");
            var cnxSlave = new Mock<IDbConnection>();
            cnxSlave.Setup(c => c.ConnectionString).Returns("slave");

            return this.CreateConnectionManager(cnxMaster.Object, cnxSlave.Object);
        }

        private IConnectionManager CreateConnectionManager(IDbConnection masterCnx,
            IDbConnection slaveCnx)
        {
            bool test;
            var cnxManager = new Mock<IConnectionManager>();
            cnxManager.Setup(c => c.GetWritableConnection())
                .Returns(masterCnx);
            cnxManager.Setup(c => c.GetReadableConnection())
                .Returns(slaveCnx);
            cnxManager.Setup(c => c.GetReadableConnection(out test))
                .Returns(slaveCnx);

            return cnxManager.Object;
        }

        [Fact]
        public void WhenCallingGetWritable_ThenGetConnectionGiveWritable()
        {
            var repCnx = new ReplicationConnection(this.CreateConnectionManager());
            var wCnx = repCnx.GetWritableConnection();
            var rCnx = repCnx.GetConnection();

            Assert.Equal("master", wCnx.ConnectionString);
            Assert.Equal("master", rCnx.ConnectionString);
            Assert.Equal(rCnx, wCnx);
        }

        [Fact]
        public void WhenCallingClose_ThenConnectionsAreClosed()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.Close()).Verifiable();

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            var wCnx = repCnx.GetWritableConnection();
            repCnx.Close();

            cnx.Verify(c => c.Close(), Times.Once(), "Closed not called");
        }

        [Fact]
        public void WhenCallingDispose_ThenConnectionsAreDisposed()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.Dispose()).Verifiable();

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            var wCnx = repCnx.GetWritableConnection();
            repCnx.Dispose();

            cnx.Verify(c => c.Dispose(), Times.Once(), "Dispose not called");
        }

        [Fact]
        public void WhenDontOpenBeforeGetWritable_ThenOpenIsNotCalled()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.Open()).Verifiable();
            cnx.Setup(c => c.State).Returns(ConnectionState.Closed);

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            var wCnx = repCnx.GetWritableConnection();

            cnx.Verify(c => c.Open(), Times.Never(), "Open should not have been called");
        }

        [Fact]
        public void WhenOpenBeforeGetWritable_ThenOpenIsCalled()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.Open()).Verifiable();
            cnx.Setup(c => c.State).Returns(ConnectionState.Closed);

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            repCnx.Open();
            var wCnx = repCnx.GetWritableConnection();

            cnx.Verify(c => c.Open(), Times.Once(), "Open should have been called");
        }

        [Fact]
        public void WhenDontOpenBeforeGetConnection_ThenOpenIsNotCalled()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.Open()).Verifiable();
            cnx.Setup(c => c.State).Returns(ConnectionState.Closed);

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            var wCnx = repCnx.GetConnection();

            cnx.Verify(c => c.Open(), Times.Never(), "Open should not have been called");
        }

        [Fact]
        public void WhenOpenBeforeGetConnection_ThenOpenIsCalled()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.Open()).Verifiable();
            cnx.Setup(c => c.State).Returns(ConnectionState.Closed);

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            repCnx.Open();
            var wCnx = repCnx.GetConnection();

            cnx.Verify(c => c.Open(), Times.Once(), "Open should have been called");
        }

        [Fact]
        public void WhenChangeDatabaseBeforeGetWritable_ThenChangeDatabaseIsCalled()
        {
            string dbName = "my_test";
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.ChangeDatabase(dbName)).Verifiable();

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            repCnx.ChangeDatabase(dbName);
            var wCnx = repCnx.GetWritableConnection();

            cnx.Verify(c => c.ChangeDatabase(dbName), Times.Once(), "Change database should have been called");
        }

        [Fact]
        public void WhenChangeDatabaseBeforeGetConnection_ThenChangeDatabaseIsCalled()
        {
            string dbName = "my_test";
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.ChangeDatabase(dbName)).Verifiable();

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            repCnx.ChangeDatabase(dbName);
            var wCnx = repCnx.GetConnection();

            cnx.Verify(c => c.ChangeDatabase(dbName), Times.Once(), "Change database should have been called");
        }

        [Fact]
        public void WhenBeginTransactionBeforeGetWritable_ThenBeginTransactionIsCalled()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.BeginTransaction()).Verifiable();

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            var trans = repCnx.BeginTransaction();
            cnx.Verify(c => c.BeginTransaction(), Times.Never(), "Begin transaction should not have been called");
            var wCnx = repCnx.GetWritableConnection();

            cnx.Verify(c => c.BeginTransaction(), Times.Once(), "Begin transaction should have been called");
        }

        [Fact]
        public void WhenBeginTransactionBeforeGetConnection_ThenBeginTransactionIsCalled()
        {
            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.BeginTransaction()).Verifiable();

            var cnxManager = this.CreateConnectionManager(cnx.Object, cnx.Object);

            var repCnx = new ReplicationConnection(cnxManager);
            repCnx.BeginTransaction();
            var wCnx = repCnx.GetConnection();

            cnx.Verify(c => c.BeginTransaction(), Times.Once(), "Begin transaction should have been called");
        }
    }
}
