using System;
using System.Data;

using Npgsql;
using Xunit;

namespace NpgsqlTests
{
    /// <summary>
    /// Summary description for PrepareTest.
    /// </summary>
    public class PrepareTest : BaseClassTests
    {
        protected override IDbConnection TheConnection
        {
            get { return _conn; }
        }
        protected override IDbTransaction TheTransaction {
            get { return _t; }
            set { _t = value; }
        }
        protected override void SetUp()
        {
            base.SetUp();
            string sql = @"	CREATE TABLE public.preparetest
                         (
                         testid serial NOT NULL,
                         varchar_notnull varchar(100) NOT NULL,
                         varchar_null varchar(100),
                         integer_notnull int4 NOT NULL,
                         integer_null int4,
                         bigint_notnull int8 NOT NULL,
                         bigint_null int8
                         ) WITHOUT OIDS;";
            var cmd = TheConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
            CommitTransaction = true;
        }


        
        protected override void TearDown()
        {
            
            string sql = @"	DROP TABLE public.preparetest;";
            var cmd = TheConnection.CreateCommand();
            cmd.CommandText = sql;

            cmd.ExecuteNonQuery();
            CommitTransaction = true;
            base.TearDown();
        }
        

        [Fact]
        public void TestInt8Null()
        {
            var cmd = GetCommand();


            // Default params work OK
            cmd.ExecuteNonQuery();

            (cmd.Parameters[5] as IDbDataParameter).Value = System.DBNull.Value;
            //cmd.Parameters[5].Value = null;

            // This too
            cmd.ExecuteNonQuery();

            cmd.Prepare();

            // This will fail
            cmd.ExecuteNonQuery();
        }

        [Fact]
        public void TestInt4Null()
        {
            var cmd = GetCommand();


            // Default params work OK
            cmd.ExecuteNonQuery();

            (cmd.Parameters[3] as IDbDataParameter).Value = System.DBNull.Value;

            // This too
            cmd.ExecuteNonQuery();

            cmd.Prepare();

            // This will fail
            cmd.ExecuteNonQuery();
        }

        [Fact]
        public void TestVarcharNull()
        {
            var cmd = GetCommand();


            // Default params work OK
            cmd.ExecuteNonQuery();

            (cmd.Parameters[1] as IDbDataParameter).Value = System.DBNull.Value;

            // This too
            cmd.ExecuteNonQuery();

            cmd.Prepare();

            // This inserts a string with the value 'NULL'
            cmd.ExecuteNonQuery();
        }

        private IDbCommand GetCommand()
        {
            string sql = @"	INSERT INTO preparetest(varchar_notnull, varchar_null, integer_notnull, integer_null, bigint_notnull, bigint_null)
                         VALUES(:param1, :param2, :param3, :param4, :param5, :param6)";
            var cmd = TheConnection.CreateCommand();
            cmd.CommandText = sql;

            NpgsqlParameter p1 = new NpgsqlParameter("param1", DbType.String, 100);
            p1.Value = "One";
            cmd.Parameters.Add(p1);
            NpgsqlParameter p2 = new NpgsqlParameter("param2", DbType.String, 100);
            p2.Value = "Two";
            cmd.Parameters.Add(p2);
            NpgsqlParameter p3 = new NpgsqlParameter("param3", DbType.Int32);
            p3.Value = 3;
            cmd.Parameters.Add(p3);
            NpgsqlParameter p4 = new NpgsqlParameter("param4", DbType.Int32);
            p4.Value = 4;
            cmd.Parameters.Add(p4);
            NpgsqlParameter p5 = new NpgsqlParameter("param5", DbType.Int64);
            p5.Value = 5;
            cmd.Parameters.Add(p5);
            NpgsqlParameter p6 = new NpgsqlParameter("param6", DbType.Int64);
            p6.Value = 6;
            cmd.Parameters.Add(p6);

            return cmd;
        }

        [Fact]
        public void TestSubquery()
        {
            string sql = "SELECT testid FROM preparetest WHERE :p1 IN (SELECT varchar_notnull FROM preparetest)";
            var cmd = TheConnection.CreateCommand();
            cmd.CommandText = sql;
            NpgsqlParameter p1 = new NpgsqlParameter(":p1", DbType.String);
            p1.Value = "blahblah";
            cmd.Parameters.Add(p1);


            cmd.ExecuteNonQuery(); // Succeeds

            cmd.Prepare(); // Fails

            cmd.ExecuteNonQuery();
        }
    }
    public class PrepareTestV2 : PrepareTest
    {
        protected override IDbConnection TheConnection
        {
            get { return _connV2; }
        }
        protected override IDbTransaction TheTransaction {
            get { return _tV2; }
        }
    }
}
