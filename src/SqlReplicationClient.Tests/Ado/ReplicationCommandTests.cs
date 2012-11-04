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
    public class TestableReplicationCommand : ReplicationCommand
    {
        public TestableReplicationCommand(ReplicationConnection connection,
            ICommandTypeAnalyser cmdTypeAnalyser) :
            base(connection, cmdTypeAnalyser)
        {
        }

        public IDbCommand TestableCreateCommand(IDbConnection cnx)
        {
            return this.CreateCommand(cnx);
        }

        public void SetCurrentCommand(IDbCommand cmd)
        {
            this.currentCommand = cmd;
        }
    }

    public class ReplicationCommandTests
    {
        [Fact]
        public void WhenCreateCommand_ThenAllDataIsCopied()
        {
            var paramList = new List<IDbDataParameter>();

            var mCmd = new Mock<IDbCommand>();
            mCmd.SetupAllProperties();
            mCmd.Setup(c => c.CreateParameter()).Returns(() =>
            {
                var pMock = new Mock<IDbDataParameter>();
                pMock.SetupAllProperties();
                return pMock.Object;
            });

            var paramCol = new Mock<IDataParameterCollection>();
            paramCol.Setup(c => c.Add(It.IsAny<object>()))
                .Callback((object c) => paramList.Add(c as IDbDataParameter));
            paramCol.Setup(c => c.Count).Returns(() => paramList.Count);
            paramCol.Setup(c => c[It.IsAny<int>()]).Returns((int i) => paramList[i]);

            mCmd.Setup(c => c.Parameters).Returns(paramCol.Object);

            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.CreateCommand()).Returns(new System.Data.SqlClient.SqlCommand());

            var rpCmd = new TestableReplicationCommand(new ReplicationConnection(null, null)
                , null);
            rpCmd.SetCurrentCommand(mCmd.Object);

            rpCmd.CommandText = "my cmd text";
            rpCmd.CommandTimeout = 127;
            rpCmd.CommandType = CommandType.StoredProcedure;
            rpCmd.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;

            for (int i = 0; i < 7; i++)
            {
                var param = rpCmd.CreateParameter();
                param.ParameterName = "test" + i;
                rpCmd.Parameters.Add(param);
            }

            var testCmd = rpCmd.TestableCreateCommand(cnx.Object);

            Assert.Equal(rpCmd.CommandText, testCmd.CommandText);
            Assert.Equal(rpCmd.CommandTimeout, testCmd.CommandTimeout);
            Assert.Equal(rpCmd.CommandType, testCmd.CommandType);
            Assert.Equal(rpCmd.UpdatedRowSource, testCmd.UpdatedRowSource);
            Assert.Equal(7, testCmd.Parameters.Count);
            for (int i = 0; i < 7; i++)
            {
                Assert.Equal("test" + i, (testCmd.Parameters[i] as IDbDataParameter).ParameterName);
            }
        }

        [Fact]
        public void WhenCallCancel_ThenCancelIsCalled()
        {
            var mCmd = new Mock<IDbCommand>();
            mCmd.Setup(c => c.Cancel()).Verifiable();

            var rpCmd = new TestableReplicationCommand(null, null);
            rpCmd.SetCurrentCommand(mCmd.Object);
            rpCmd.Cancel();

            mCmd.Verify(c => c.Cancel(), Times.Once(), "Cancel should have been called");
        }

        [Fact]
        public void WhenCallPrepare_ThenPrepareIsCalled()
        {
            var mCurrentCmd = new Mock<IDbCommand>();
            mCurrentCmd.SetupAllProperties();
            mCurrentCmd.Setup(c => c.Prepare()).Verifiable();

            var cnx = new Mock<IDbConnection>();
            cnx.Setup(c => c.CreateCommand()).Returns(mCurrentCmd.Object);

            var rpCmd = new TestableReplicationCommand(new ReplicationConnection(null, null)
                , null);
            rpCmd.SetCurrentCommand(mCurrentCmd.Object);

            rpCmd.Prepare();
            var testCmd = rpCmd.TestableCreateCommand(cnx.Object);

            mCurrentCmd.Verify(c => c.Prepare(), Times.Once(), "Prepare should have been called");
        }

        [Fact]
        public void WhenCallDispose_ThenDisposeIsCalled()
        {
            var mCmd1 = new Mock<IDbCommand>();
            mCmd1.Setup(c => c.Dispose()).Verifiable();

            var rpCmd = new TestableReplicationCommand(null, null);
            rpCmd.SetCurrentCommand(mCmd1.Object);
            rpCmd.Dispose();

            mCmd1.Verify(c => c.Dispose(), Times.Once(), "Dispose should have been called");
        }
    }
}
