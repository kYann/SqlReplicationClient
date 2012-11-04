using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Sessions
{
    public class Session
    {
        private bool isInWritableMode = false;

        public void SetWritableMode()
        {
            this.isInWritableMode = true;
        }

        public void SetClassicMode()
        {
            this.isInWritableMode = false;
        }

        public bool IsInWritableMode()
        {
            return this.isInWritableMode;
        }
    }
}
