using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace SqlReplicationClient.Sessions
{
    public class WebSessionManager : SessionManager
    {
        const string contextKey = "SqlReplicationSession_";

        public override Session GetCurrentSession()
        {
            Session session = null;
            if (HttpContext.Current.Items.Contains(contextKey))
                session = (Session)HttpContext.Current.Items[contextKey];
            if (session == null)
                session = new Session();
            return session;
        }
    }
}