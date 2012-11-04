using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace SqlReplicationClient.Sessions
{
    public class ThreadSessionManager : SessionManager
    {
        [ThreadStatic]
        static Session session;

        public override Session GetCurrentSession()
        {
            if (session == null)
                session = new Session();
            return session;
        }
    }
}
