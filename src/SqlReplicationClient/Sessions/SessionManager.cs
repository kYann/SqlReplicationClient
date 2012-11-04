using SqlReplicationClient.Servers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Sessions
{
    public class SessionManager : ISessionManager
    {
        static ISessionManager sessionManager;

        public static ISessionManager Current 
        {
            get { return sessionManager; }
        }

        public static void SetSessionManager(ISessionManager sessionManager)
        {
            SessionManager.sessionManager = sessionManager;
        }

        public virtual Session GetCurrentSession()
        {
            throw new NotImplementedException();
        }

        public void SetSessionOnWritable()
        {
            var session = this.GetCurrentSession();
            session.SetWritableMode();
        }

        public void SetSessionOnClassicMode()
        {
            var session = this.GetCurrentSession();
            session.SetClassicMode();
        }

        public IDisposable WritableScope()
        {
            var session = this.GetCurrentSession();
            var sessionScope = new SessionScope(session);
            session.SetWritableMode();
            return sessionScope;
        }

        public IDisposable ReadableScope()
        {
            var session = this.GetCurrentSession();
            var sessionScope = new SessionScope(session);
            session.SetClassicMode();
            return sessionScope;
        }
    }
}
