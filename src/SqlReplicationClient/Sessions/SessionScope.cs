using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Sessions
{
    public class SessionScope : IDisposable
    {
        readonly Session session;
        readonly bool isWritableMode;

        public SessionScope(Session session)
        {
            this.session = session;
            this.isWritableMode = session.IsInWritableMode();
        }

        public void Dispose()
        {
            if (isWritableMode)
                session.SetWritableMode();
            else
                session.SetClassicMode();
        }
    }
}
