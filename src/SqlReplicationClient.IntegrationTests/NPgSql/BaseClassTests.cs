//
// Author:
//  Francisco Figueiredo Jr. <fxjrlists@yahoo.com>
//
//  Copyright (C) 2002-2005 The Npgsql Development Team
//  npgsql-general@gborg.postgresql.org
//  http://gborg.postgresql.org/project/npgsql/projdisplay.php
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


using System;
using System.Data;
using System.Configuration;
using Npgsql;

using NpgsqlTypes;
using SqlReplicationClient.Ado;
using SqlReplicationClient;
using SqlReplicationClient.Servers;
using SqlReplicationClient.Servers.DriversImpl;
using SqlReplicationClient.Sessions;
using System.IO;

namespace NpgsqlTests
{

    public abstract class BaseClassTests : IDisposable
    {
        
        // Connection tests will use.
        protected IDbConnection _conn = null;
        protected IDbConnection _connV2 = null;
        
        // Transaction to rollback tests modifications.
        protected IDbTransaction _t = null;
        protected IDbTransaction _tV2 = null;

        protected abstract IDbConnection TheConnection { get; }
        protected abstract IDbTransaction TheTransaction { get; set; }
        
        // Commit transaction when test finish?   
        private Boolean commitTransaction = false;
        
        // Connection string
        
        protected String _connString = ConfigurationManager.ConnectionStrings["ConnectionString"]
            .ConnectionString;
        protected string _connV2String = ConfigurationManager.ConnectionStrings["ConnectionStringV2"]
            .ConnectionString;
        
        protected Boolean CommitTransaction
        {
            set
            {
                commitTransaction = value;
            }
            
            get
            {
                return commitTransaction;
            }
        }

        public BaseClassTests()
        {
            this.TearDownData();
            this.SetUpData();
            this.SetUp();
        }

        protected virtual void SetUpData()
        {
            using (var cnx = new NpgsqlConnection(_connString))
            {
                cnx.Open();
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = File.ReadAllText("../../../../sql/test/pgsql/add_data.sql");
                    cmd.ExecuteNonQuery();
                }
            }
        }

        protected virtual void TearDownData()
        {
            using (var cnx = new NpgsqlConnection(_connString))
            {
                cnx.Open();
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = File.ReadAllText("../../../../sql/test/pgsql/clean_data.sql");
                    cmd.ExecuteNonQuery();
                }
            }
        }

        protected IDbConnection CreateEnlistConnection(bool pooling = false)
        {
            return new NpgsqlConnection(_connString + ";enlist=true;pooling="
                + pooling.ToString().ToLower());
        }

        protected IDbConnection CreateTimeoutConnection(int timeout)
        {
            return new NpgsqlConnection(_connString + ";CommandTimeout=" + timeout + ";pooling=false");
        }
                
        protected virtual void SetUp()
        {
            Func<IDbConnection> cnxFactory = () => new NpgsqlConnection();

            SessionManager.SetSessionManager(new ThreadSessionManager());
            var sm1 = ServerManager.Start(new DefaultNpgsqlServerChecker(cnxFactory), 
                new string[]{ _connString });
            
            _conn = new ReplicationConnection(
                new DefaultConnectionManager(() => new NpgsqlConnection(), sm1.GetActiveServers()));
            //_conn = new NpgsqlConnection(_connString);
            _conn.Open();
            _t = _conn.BeginTransaction();

            var sm2 = ServerManager.Start(new DefaultNpgsqlServerChecker(cnxFactory),
                new string[] { _connV2String });

            _connV2 = new ReplicationConnection(
                new DefaultConnectionManager(() => new NpgsqlConnection(), sm2.GetActiveServers()));
            //_connV2 = new NpgsqlConnection(_connV2String);

            _connV2.Open();
            _tV2 = _connV2.BeginTransaction();
            
            CommitTransaction = false;
        }

        protected virtual void TearDown()
        {
            ServerManager.StopAll();
            if (_t != null && _t.Connection != null)
                if (CommitTransaction)
                    _t.Commit();
                else
                    _t.Rollback();
                
            if (_conn.State != ConnectionState.Closed)
                _conn.Close();
            _conn.Dispose();
            if (_tV2 != null && _tV2.Connection != null)
                if(CommitTransaction)
                    _tV2.Commit();
                else
                    _tV2.Rollback();
                
            if (_connV2.State != ConnectionState.Closed)
                _connV2.Close();
            _connV2.Dispose();
        }


        public void Dispose()
        {
            this.TearDown();
        }
    }
}
