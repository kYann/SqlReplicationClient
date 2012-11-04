using SqlReplicationClient.Sessions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SqlReplicationClient.Tests.Sessions
{
    public class SessionScopeTests
    {
        [Fact]
        public void WhenUsingSessionScopeFromReadToWrite_ThenStateGoBackToRead()
        {
            var session = new Session();
            session.SetClassicMode();

            using (var scope = new SessionScope(session))
            {
                session.SetWritableMode();
            }

            Assert.False(session.IsInWritableMode());
        }

        [Fact]
        public void WhenUsingSessionScopeFromWriteToRead_ThenStateGoBackToWrite()
        {
            var session = new Session();
            session.SetWritableMode();

            using (var scope = new SessionScope(session))
            {
                session.SetClassicMode();
            }

            Assert.True(session.IsInWritableMode());
        }
    }
}
