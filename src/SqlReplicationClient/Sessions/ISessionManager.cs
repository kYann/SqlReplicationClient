using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Sessions
{
    public interface ISessionManager
    {
        Session GetCurrentSession();

        void SetSessionOnWritable();

        void SetSessionOnClassicMode();

        IDisposable WritableScope();

        IDisposable ReadableScope();
    }
}
