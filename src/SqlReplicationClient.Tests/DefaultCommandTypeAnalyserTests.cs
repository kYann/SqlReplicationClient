using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SqlReplicationClient.Tests
{
    public class DefaultCommandTypeAnalyserTests
    {
        [Fact]
        public void WhenUsingExecuteNonQuery_ThenCommandTypeIsWrite()
        {
            var cmdTypeAnalyser = new DefaultCommandTypeAnalyser();

            var result = cmdTypeAnalyser.GetReplicationCommandType(null, CommandFunction.ExecuteNonQuery);

            Assert.Equal(ReplicationCommandType.Write, result);
        }

        [Fact]
        public void WhenUsingExecuteReader_ThenCommandTypeIsRead()
        {
            var cmdTypeAnalyser = new DefaultCommandTypeAnalyser();

            var result = cmdTypeAnalyser.GetReplicationCommandType(null, CommandFunction.ExecuteReader);

            Assert.Equal(ReplicationCommandType.Read, result);
        }

        [Fact]
        public void WhenUsingExecuteScalar_ThenCommandTypeIsRead()
        {
            var cmdTypeAnalyser = new DefaultCommandTypeAnalyser();

            var result = cmdTypeAnalyser.GetReplicationCommandType(null, CommandFunction.ExecuteScalar);

            Assert.Equal(ReplicationCommandType.Read, result);
        }
    }
}
