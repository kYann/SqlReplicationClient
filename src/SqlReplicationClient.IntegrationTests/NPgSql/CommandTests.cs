// CommandTests.cs created with MonoDevelop
// User: fxjr at 11:40 PM 4/9/2008
//
// To change standard headers go to Edit->Preferences->Coding->Standard Headers
//
// created on 30/11/2002 at 22:35
//
// Author:
//     Francisco Figueiredo Jr. <fxjrlists@yahoo.com>
//
//    Copyright (C) 2002 The Npgsql Development Team
//    npgsql-general@gborg.postgresql.org
//    http://gborg.postgresql.org/project/npgsql/projdisplay.php
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

using Npgsql;
using System.Data;
using System.Globalization;
using System.Net;
using NpgsqlTypes;
using System.Resources;
using System.Threading;
using Xunit;

namespace NpgsqlTests
{

    public enum EnumTest : short
    {
        Value1 = 0,
        Value2 = 1
    };

    public class CommandTests : BaseClassTests
    {
        protected override IDbConnection TheConnection {
            get { return _conn;}
        }
        protected override IDbTransaction TheTransaction
        {
            get { return _t; }
            set { _t = value; }
        }
        protected virtual string TheConnectionString {
            get { return _connString; }
        }

        protected NpgsqlParameter GetNpDbParam(object dbparam)
        {
            return dbparam as NpgsqlParameter;
        }

        [Fact]
        public void ParametersGetName()
        {
            var command = this.TheConnection.CreateCommand();

            // Add parameters.
            command.Parameters.Add(new NpgsqlParameter(":Parameter1", DbType.Boolean));
            command.Parameters.Add(new NpgsqlParameter(":Parameter2", DbType.Int32));
            command.Parameters.Add(new NpgsqlParameter(":Parameter3", DbType.DateTime));
            command.Parameters.Add(new NpgsqlParameter("Parameter4", DbType.DateTime));

            IDbDataParameter idbPrmtr = command.Parameters["Parameter1"] as IDbDataParameter;
            Assert.NotNull(idbPrmtr);
            GetNpDbParam(command.Parameters[0]).Value = 1;

            // Get by indexers.

            Assert.Equal(":Parameter1", GetNpDbParam(command.Parameters[":Parameter1"]).ParameterName);
            Assert.Equal(":Parameter2", GetNpDbParam(command.Parameters[":Parameter2"]).ParameterName);
            Assert.Equal(":Parameter3", GetNpDbParam(command.Parameters[":Parameter3"]).ParameterName);
            //Assert.Equal(":Parameter4", GetNpDbParam(command.Parameters["Parameter4"]).ParameterName); //Should this work?

            Assert.Equal(":Parameter1", GetNpDbParam(command.Parameters[0]).ParameterName);
            Assert.Equal(":Parameter2", GetNpDbParam(command.Parameters[1]).ParameterName);
            Assert.Equal(":Parameter3", GetNpDbParam(command.Parameters[2]).ParameterName);
            Assert.Equal("Parameter4", GetNpDbParam(command.Parameters[3]).ParameterName);
        }

        [Fact]
        public void ParameterNameWithSpace()
        {
            var command = this.TheConnection.CreateCommand();

            // Add parameters.
            command.Parameters.Add(new NpgsqlParameter(":Parameter1 ", DbType.Boolean));
            
            Assert.Equal(":Parameter1", GetNpDbParam(command.Parameters[0]).ParameterName);
            
        }

        [Fact]
        public void EmptyQuery()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = ";";
            command.ExecuteNonQuery();
        }
        
        
        [Fact]
        public void NoNameParameterAdd()
        {
            var command = this.TheConnection.CreateCommand();

            command.Parameters.Add(new NpgsqlParameter());
            command.Parameters.Add(new NpgsqlParameter());
            
            
            Assert.Equal(":Parameter1", GetNpDbParam(GetNpDbParam(command.Parameters[0])).ParameterName);
            Assert.Equal(":Parameter2", GetNpDbParam(command.Parameters[1]).ParameterName);
        }
        

        [Fact]
        public void FunctionCallFromSelect()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select * from funcB()";

            var reader = command.ExecuteReader();

            Assert.NotNull(reader);
            reader.Close();
            //reader.FieldCount
        }

        [Fact]
        public void ExecuteScalar()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select count(*) from tablea";

            Object result = command.ExecuteScalar();

            Assert.Equal(6, Convert.ToInt32(result));
            //reader.FieldCount
        }
        
        [Fact]
        public void TransactionSetOk()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select count(*) from tablea";
            
            command.Transaction = TheTransaction;
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(6, Convert.ToInt32(result));
        }
        
        
        [Fact]
        public void InsertStringWithBackslashes()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:p0)";
            
            command.Parameters.Add(new NpgsqlParameter("p0", NpgsqlDbType.Text));
            
            (GetNpDbParam(command.Parameters["p0"]) as IDbDataParameter).Value = @"\test";

            Object result = command.ExecuteNonQuery();

            Assert.Equal(1, Convert.ToInt32(result));
            
            
            var command2 = this.TheConnection.CreateCommand();
            command2.CommandText = "select field_text from tablea where field_serial = (select max(field_serial) from tablea)";
            

            result = command2.ExecuteScalar();
            
            Assert.Equal(@"\test", result);
            
            
            
            //reader.FieldCount
        }
               
        
        [Fact]
        public void UseStringParameterWithNoNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:p0)";
            
            command.Parameters.Add(new NpgsqlParameter("p0", "test"));
            
            
            Assert.Equal(GetNpDbParam(command.Parameters[0]).NpgsqlDbType, NpgsqlDbType.Text);
            Assert.Equal(GetNpDbParam(command.Parameters[0]).DbType, DbType.String);
            
            Object result = command.ExecuteNonQuery();

            Assert.Equal(1, Convert.ToInt32(result));
            
            
            var command2 = this.TheConnection.CreateCommand();
            command2.CommandText = "select field_text from tablea where field_serial = (select max(field_serial) from tablea)";
            

            result = command2.ExecuteScalar();
            
            
            
            Assert.Equal("test", result);
            
            
            
            //reader.FieldCount
        }
        
        
        [Fact]
        public void UseIntegerParameterWithNoNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_int4) values (:p0)";
            
            command.Parameters.Add(new NpgsqlParameter("p0", 5));
            
            Assert.Equal(GetNpDbParam(command.Parameters[0]).NpgsqlDbType, NpgsqlDbType.Integer);
            Assert.Equal(GetNpDbParam(command.Parameters[0]).DbType, DbType.Int32);
            
            
            Object result = command.ExecuteNonQuery();

            Assert.Equal(1, result);
            
            
            var command2 = this.TheConnection.CreateCommand();
            command2.CommandText = "select field_int4 from tablea where field_serial = (select max(field_serial) from tablea)";
            

            result = command2.ExecuteScalar();
            
            
            Assert.Equal(5, Convert.ToInt32(result));
            
            
           //reader.FieldCount
        }
        
        
        //[Fact]
        public void UseSmallintParameterWithNoNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_int4) values (:p0)";
            
            command.Parameters.Add(new NpgsqlParameter("p0", (Int16)5));
            
            Assert.Equal(GetNpDbParam(command.Parameters[0]).NpgsqlDbType, NpgsqlDbType.Smallint);
            Assert.Equal(GetNpDbParam(command.Parameters[0]).DbType, DbType.Int16);
            
            
            Object result = command.ExecuteNonQuery();

            Assert.Equal(1, result);
            
            
            var command2 = this.TheConnection.CreateCommand();
            command2.CommandText = "select field_int4 from tablea where field_serial = (select max(field_serial) from tablea)";

            result = command2.ExecuteScalar();
            
            
            Assert.Equal(5, Convert.ToInt32(result));
            
            
            //reader.FieldCount
        }
        
        
        [Fact]
        public void FunctionCallReturnSingleValue()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC();";
            command.CommandType = CommandType.StoredProcedure;

            Object result = command.ExecuteScalar();

            Assert.Equal(6, Convert.ToInt32(result));
            //reader.FieldCount
        }
        
        
        //[Fact]
        //public void RollbackWithNoTransaction()
        //{

        //    Assert.Throws<InvalidOperationException>(() =>
        //        {
        //            TheTransaction.Rollback();
        //            TheTransaction.Rollback();
        //        });
        //}


        [Fact]
        public void FunctionCallReturnSingleValueWithPrepare()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC()";
            command.CommandType = CommandType.StoredProcedure;

            command.Prepare();
            Object result = command.ExecuteScalar();

            Assert.Equal(6, Convert.ToInt32(result));
            //reader.FieldCount
        }

        [Fact]
        public void FunctionCallWithParametersReturnSingleValue()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC(:a)";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));

            GetNpDbParam(command.Parameters[0]).Value = 4;

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }

        [Fact]
        public void FunctionCallWithParametersReturnSingleValueNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC(:a)";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Integer));

            GetNpDbParam(command.Parameters[0]).Value = 4;

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }


        [Fact]
        public void FunctionCallWithParametersPrepareReturnSingleValue()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC(:a)";
            command.CommandType = CommandType.StoredProcedure;


            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));

            Assert.Equal(1, command.Parameters.Count);
            command.Prepare();


            GetNpDbParam(command.Parameters[0]).Value = 4;

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }

        [Fact]
        public void FunctionCallWithParametersPrepareReturnSingleValueNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC(:a)";
            command.CommandType = CommandType.StoredProcedure;


            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Integer));

            Assert.Equal(1, command.Parameters.Count);
            command.Prepare();


            GetNpDbParam(command.Parameters[0]).Value = 4;

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }


        [Fact]
        public void FunctionCallWithParametersPrepareReturnSingleValueNpgsqlDbType2()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC(@a)";
            command.CommandType = CommandType.StoredProcedure;


            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Integer));

            Assert.Equal(1, command.Parameters.Count);
            //command.Prepare();


            GetNpDbParam(command.Parameters[0]).Value = 4;

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }


        [Fact]
        public void FunctionCallReturnResultSet()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcB()";
            command.CommandType = CommandType.StoredProcedure;

            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            dr.Close();
        }


        [Fact]
        public void CursorStatement()
        {
            Int32 i = 0;

            
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "declare te cursor for select * from tablea;";

            command.ExecuteNonQuery();

            command.CommandText = "fetch forward 3 in te;";

            var dr = command.ExecuteReader();


            while (dr.Read())
            {
                i++;
            }

            Assert.Equal(3, i);


            i = 0;

            command.CommandText = "fetch backward 1 in te;";

            var dr2 = command.ExecuteReader();

            while (dr2.Read())
            {
                i++;
            }

            Assert.Equal(1, i);

            command.CommandText = "close te;";

            command.ExecuteNonQuery();
        }

        [Fact]
        public void PreparedStatementNoParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select * from tablea;";

            command.Prepare();
            
            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            
            dr.Close();
        }
        
        
        [Fact]
        public void PreparedStatementInsert()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:p0);";
            command.Parameters.Add(new NpgsqlParameter("p0", NpgsqlDbType.Text));
            GetNpDbParam(command.Parameters["p0"]).Value = "test";
            
            command.Prepare();
            
            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
        }
        
        [Fact]
        public void RTFStatementInsert()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:p0);";
            command.Parameters.Add(new NpgsqlParameter("p0", NpgsqlDbType.Text));
            GetNpDbParam(command.Parameters["p0"]).Value = @"{\rtf1\ansi\ansicpg1252\uc1 \deff0\deflang1033\deflangfe1033{";
                       
            
            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            
            
            var command2 = this.TheConnection.CreateCommand();
            command2.CommandText = "select field_text from tablea where field_serial = (select max(field_serial) from tablea);";
            String result = (string)command2.ExecuteScalar();
            
            Assert.Equal(@"{\rtf1\ansi\ansicpg1252\uc1 \deff0\deflang1033\deflangfe1033{", result);
        }
        
        
        
        [Fact]
        public void PreparedStatementInsertNullValue()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_int4) values (:p0);";
            command.Parameters.Add(new NpgsqlParameter("p0", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters["p0"]).Value = DBNull.Value;

            command.Prepare();

            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
        }       

        [Fact]
        public void PreparedStatementWithParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_int4 = :a and field_int8 = :b;";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));
            command.Parameters.Add(new NpgsqlParameter("b", DbType.Int64));

            Assert.Equal(2, command.Parameters.Count);

            Assert.Equal(DbType.Int32, GetNpDbParam(command.Parameters[0]).DbType);

            command.Prepare();

            GetNpDbParam(command.Parameters[0]).Value = 3;
            GetNpDbParam(command.Parameters[1]).Value = 5;

            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            
            dr.Close();
        }

        [Fact]
        public void PreparedStatementWithParametersNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_int4 = :a and field_int8 = :b;";

            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Integer));
            command.Parameters.Add(new NpgsqlParameter("b", NpgsqlDbType.Bigint));

            Assert.Equal(2, command.Parameters.Count);

            Assert.Equal(DbType.Int32, GetNpDbParam(command.Parameters[0]).DbType);

            command.Prepare();

            GetNpDbParam(command.Parameters[0]).Value = 3;
            GetNpDbParam(command.Parameters[1]).Value = 5;

            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            dr.Close();
        }
        
        [Fact]
        public void FunctionCallWithImplicitParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC";
            command.CommandType = CommandType.StoredProcedure;


            NpgsqlParameter p = new NpgsqlParameter("@a", NpgsqlDbType.Integer);
            p.Direction = ParameterDirection.InputOutput;
            p.Value = 4;
            
            command.Parameters.Add(p);
            

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }
        
        
        [Fact]
        public void PreparedFunctionCallWithImplicitParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC";
            command.CommandType = CommandType.StoredProcedure;


            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Integer);
            p.Direction = ParameterDirection.InputOutput;
            p.Value = 4;
            
            command.Parameters.Add(p);
            
            command.Prepare();

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, Convert.ToInt32(result));
        }
        
        
        [Fact]
        public void FunctionCallWithImplicitParametersWithNoParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC";
            command.CommandType = CommandType.StoredProcedure;

            Object result = command.ExecuteScalar();

            Assert.Equal(6, Convert.ToInt32(result));
            //reader.FieldCount

        }
        
        [Fact]
        public void FunctionCallOutputParameter()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC()";
            command.CommandType = CommandType.StoredProcedure;
            
            NpgsqlParameter p = new NpgsqlParameter("a", DbType.Int32);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
                        
            command.ExecuteNonQuery();
            
            Assert.Equal(6, Convert.ToInt32(GetNpDbParam(command.Parameters["a"]).Value));
        }
        
        [Fact]
        public void FunctionCallOutputParameter2()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC";
            command.CommandType = CommandType.StoredProcedure;
            
            NpgsqlParameter p = new NpgsqlParameter("@a", DbType.Int32);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
                        
            command.ExecuteNonQuery();
            
            Assert.Equal(6, Convert.ToInt32(GetNpDbParam(command.Parameters["@a"]).Value));
        }
        
        [Fact]
        public void OutputParameterWithoutName()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC";
            command.CommandType = CommandType.StoredProcedure;
            
            NpgsqlParameter p = (NpgsqlParameter)command.CreateParameter();
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
                        
            command.ExecuteNonQuery();
            
            Assert.Equal(6, Convert.ToInt32(GetNpDbParam(command.Parameters[0]).Value));
        }
        
        [Fact]
        public void FunctionReturnVoid()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "testreturnvoid()";
            command.CommandType = CommandType.StoredProcedure;
            command.ExecuteNonQuery();
        }
        
        [Fact]
        public void StatementOutputParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "values (4,5), (6,7)";
                        
            NpgsqlParameter p = new NpgsqlParameter("a", DbType.Int32);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
            p = new NpgsqlParameter("b", DbType.Int32);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
            
            p = new NpgsqlParameter("c", DbType.Int32);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
                        
            command.ExecuteNonQuery();
            
            // Should bear the values of the first tuple.
            Assert.Equal(4, GetNpDbParam(command.Parameters["a"]).Value);
            Assert.Equal(5, GetNpDbParam(command.Parameters["b"]).Value);
            Assert.Equal(-1, GetNpDbParam(command.Parameters["c"]).Value);
        }
        
        
        [Fact]
        public void FunctionCallInputOutputParameter()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "funcC(:a)";
            command.CommandType = CommandType.StoredProcedure;


            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Integer);
            p.Direction = ParameterDirection.InputOutput;
            p.Value = 4;
            
            command.Parameters.Add(p);
            

            Int64 result = (Int64) command.ExecuteScalar();

            Assert.Equal(1, result);
        }
        
        
        [Fact]
        public void StatementMappedOutputParameters()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select 3, 4 as param1, 5 as param2, 6;";
                        
            NpgsqlParameter p = new NpgsqlParameter("param2", NpgsqlDbType.Integer);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
            p = new NpgsqlParameter("param1", NpgsqlDbType.Integer);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
            p = new NpgsqlParameter("p", NpgsqlDbType.Integer);
            p.Direction = ParameterDirection.Output;
            p.Value = -1;
            
            command.Parameters.Add(p);
            
            
            command.ExecuteNonQuery();
            
            Assert.Equal(4, GetNpDbParam(command.Parameters["param1"]).Value);
            Assert.Equal(5, GetNpDbParam(command.Parameters["param2"]).Value);
            //Assert.Equal(-1, GetNpDbParam(command.Parameters["p"]).Value); //Which is better, not filling this or filling this with an unmapped value?
        }

        public bool RecievedNotification = false;
        private void NotificationSupportHelper(Object sender, NpgsqlNotificationEventArgs args)
        {
            RecievedNotification = true;
        }

        
        [Fact]
        public void ByteSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_int2) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Byte));

            GetNpDbParam(command.Parameters[0]).Value = 2;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.Parameters.Clear();
        }
        
        
            [Fact]
        public void ByteaSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bytea from tablef where field_serial = 1";


            Byte[] result = (Byte[]) command.ExecuteScalar();
            

            Assert.Equal(2, result.Length);
        }
        
        [Fact]
        public void ByteaInsertSupport()
        {
            Byte[] toStore = { 1 };

            var cmd = this.TheConnection.CreateCommand();
            cmd.CommandText = "insert into tablef(field_bytea) values (:val)";
            cmd.Parameters.Add(new NpgsqlParameter("val", DbType.Binary));
            GetNpDbParam(cmd.Parameters[0]).Value = toStore;
            cmd.ExecuteNonQuery();

            cmd = this.TheConnection.CreateCommand();
            cmd.CommandText = "select field_bytea from tablef where field_serial = (select max(field_serial) from tablef)";
            
            Byte[] result = (Byte[])cmd.ExecuteScalar();
            
            Assert.Equal(1, result.Length);

        }        
        
        
        [Fact]
        public void ByteaParameterSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bytea from tablef where field_bytea = :bytesData";
            
            Byte[] bytes = new Byte[]{45,44};
            
            command.Parameters.Add(new NpgsqlParameter(":bytesData", NpgsqlTypes.NpgsqlDbType.Bytea));
            GetNpDbParam(command.Parameters[":bytesData"]).Value = bytes;


            Object result = command.ExecuteNonQuery();
            

            Assert.Equal(-1, result);
        }
        
        [Fact]
        public void ByteaParameterWithPrepareSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bytea from tablef where field_bytea = :bytesData";
            
            Byte[] bytes = new Byte[]{45,44};
            
            command.Parameters.Add(new NpgsqlParameter(":bytesData", NpgsqlTypes.NpgsqlDbType.Bytea));
                  GetNpDbParam(command.Parameters[":bytesData"]).Value = bytes;


            command.Prepare();
            Object result = command.ExecuteNonQuery();
            

            Assert.Equal(-1, result);
        }
        
        
            [Fact]
        public void EnumSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_int2) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Smallint));

            GetNpDbParam(command.Parameters[0]).Value = EnumTest.Value1;
            

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);
        }

        [Fact]
        public void DateTimeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = 2;";

            DateTime d = (DateTime)command.ExecuteScalar();


            Assert.Equal(DateTimeKind.Unspecified, d.Kind);
            Assert.Equal("2002-02-02 09:00:23Z", d.ToString("u"));

            DateTimeFormatInfo culture = new DateTimeFormatInfo();
            culture.TimeSeparator = ":";
            DateTime dt = System.DateTime.Parse("2004-06-04 09:48:00", culture);

            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.DateTime));
            GetNpDbParam(command.Parameters[0]).Value = dt;

            command.ExecuteScalar();
       }


        [Fact]
        public void DateTimeSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = 2;";

            DateTime d = (DateTime)command.ExecuteScalar();


            Assert.Equal("2002-02-02 09:00:23Z", d.ToString("u"));

            DateTimeFormatInfo culture = new DateTimeFormatInfo();
            culture.TimeSeparator = ":";
            DateTime dt = System.DateTime.Parse("2004-06-04 09:48:00", culture);

            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Timestamp));
            GetNpDbParam(command.Parameters[0]).Value = dt;

            command.ExecuteScalar();
        }

        [Fact]
        public void DateSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_date from tablec where field_serial = 1;";

            DateTime d = (DateTime)command.ExecuteScalar();


            Assert.Equal(DateTimeKind.Unspecified, d.Kind);
            Assert.Equal("2002-03-04", d.ToString("yyyy-MM-dd"));
        }

        [Fact]
        public void TimeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_time from tablec where field_serial = 2;";

            DateTime d = (DateTime)command.ExecuteScalar();


            Assert.Equal(DateTimeKind.Unspecified, d.Kind);
            Assert.Equal("10:03:45.345", d.ToString("HH:mm:ss.fff"));
        }

        [Fact]
        public void DateTimeSupportTimezone2()
        {
            //Changed the comparison. Did thisassume too much about ToString()?
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "set time zone 5; select field_timestamp_with_timezone from tableg where field_serial = 1;";

            String s = ((DateTime)command.ExecuteScalar()).ToUniversalTime().ToString();
           
            Assert.Equal(new DateTime(2002,02,02,09,00,23).ToString() , s);
        }

        [Fact]
        public void DateTimeSupportTimezone3()
        {
            //2009-11-11 20:15:43.019-03:30
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "set time zone 5;select timestamptz'2009-11-11 20:15:43.019-03:30';";

            DateTime d = (DateTime)command.ExecuteScalar();


            Assert.Equal(DateTimeKind.Local, d.Kind);
            Assert.Equal("2009-11-11 23:45:43Z", d.ToUniversalTime().ToString("u"));

        }
        
        [Fact]
        public void DateTimeSupportTimezoneEuropeAmsterdam()
        {
            //1929-08-19 00:00:00+01:19:32
            // This test was provided by Christ Akkermans.
            
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "SET TIME ZONE 'Europe/Amsterdam';SELECT '1929-08-19 00:00:00'::timestamptz;";

            DateTime d = (DateTime)command.ExecuteScalar();
            
           

        }



        [Fact]
        public void ProviderDateTimeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = 2;";

            NpgsqlTimeStamp ts;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                ts = reader.GetTimeStamp(0);
            }

            Assert.Equal("2002-02-02 09:00:23.345", ts.ToString());

            DateTimeFormatInfo culture = new DateTimeFormatInfo();
            culture.TimeSeparator = ":";
            NpgsqlTimeStamp ts1 = NpgsqlTimeStamp.Parse("2004-06-04 09:48:00");

            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.DateTime));
            GetNpDbParam(command.Parameters[0]).Value = ts1;

            command.ExecuteScalar();
        }


        [Fact]
        public void ProviderDateTimeSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = 2;";

            NpgsqlTimeStamp ts;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                ts = reader.GetTimeStamp(0);
            }

            Assert.Equal("2002-02-02 09:00:23.345", ts.ToString());

            DateTimeFormatInfo culture = new DateTimeFormatInfo();
            culture.TimeSeparator = ":";
            NpgsqlTimeStamp ts1 = NpgsqlTimeStamp.Parse("2004-06-04 09:48:00");

            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Timestamp));
            GetNpDbParam(command.Parameters[0]).Value = ts1;

            command.ExecuteScalar();
        }

        [Fact]
        public void ProviderDateSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_date from tablec where field_serial = 1;";

            NpgsqlDate d;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                d = reader.GetDate(0);
            }

            Assert.Equal("2002-03-04", d.ToString());
        }

        [Fact]
        public void ProviderTimeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_time from tablec where field_serial = 2;";

            NpgsqlTime t;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                t = reader.GetTime(0);
            }


            Assert.Equal("10:03:45.345", t.ToString());
        }

        [Fact]
        public void ProviderTimeSupportTimezone()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select '13:03:45.001-05'::timetz";

            NpgsqlTimeTZ t;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                t = reader.GetTimeTZ(0);
            }

            Assert.Equal("18:03:45.001", t.AtTimeZone(NpgsqlTimeZone.UTC).LocalTime.ToString());
        }

        [Fact]
        public void ProviderDateTimeSupportTimezone()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "set time zone 5;select field_timestamp_with_timezone from tableg where field_serial = 1;";

            NpgsqlTimeStampTZ ts;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                ts = reader.GetTimeStampTZ(0);
            }

            Assert.Equal("2002-02-02 09:00:23.345", ts.AtTimeZone(NpgsqlTimeZone.UTC).ToString());
        }

        [Fact]
        public void ProviderDateTimeSupportTimezone3()
        {
            //2009-11-11 20:15:43.019-03:30
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "set time zone 5;select timestamptz'2009-11-11 20:15:43.019-03:30';";

            NpgsqlTimeStampTZ ts;
            using (var reader = command.ExecuteReader() as NpgsqlDataReader)
            {
                reader.Read();
                ts = reader.GetTimeStampTZ(0);
            }

            Assert.Equal("2009-11-12 04:45:43.019+05", ts.ToString());

        }

        [Fact]
        public void NumericSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_numeric) values (:a)";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Decimal));

            GetNpDbParam(command.Parameters[0]).Value = 7.4M;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select * from tableb where field_numeric = :a";


            var dr = command.ExecuteReader();
            dr.Read();

            Decimal result = dr.GetDecimal(3);


            Assert.Equal(7.4000000M, result);
            dr.Close();
        }

        [Fact]
        public void NumericSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_numeric) values (:a)";
            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Numeric));

            GetNpDbParam(command.Parameters[0]).Value = 7.4M;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select * from tableb where field_numeric = :a";


            var dr = command.ExecuteReader();
            dr.Read();

            Decimal result = dr.GetDecimal(3);


            Assert.Equal(7.4000000M, result);
            
            dr.Close();
        }


        [Fact]
        public void InsertSingleValue()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tabled(field_float4) values (:a)";
            command.Parameters.Add(new NpgsqlParameter(":a", DbType.Single));

            GetNpDbParam(command.Parameters[0]).Value = 7.4F;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select * from tabled where field_float4 = :a";


            var dr = command.ExecuteReader();
            dr.Read();

            Single result = dr.GetFloat(1);


            Assert.Equal(7.4F, result);
            
            dr.Close();
        }


        [Fact]
        public void InsertSingleValueNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tabled(field_float4) values (:a)";
            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Real));

            GetNpDbParam(command.Parameters[0]).Value = 7.4F;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select * from tabled where field_float4 = :a";


            var dr = command.ExecuteReader();
            dr.Read();

            Single result = dr.GetFloat(1);


            Assert.Equal(7.4F, result);
            
            dr.Close();
        }
        
        
        [Fact]
        public void DoubleValueSupportWithExtendedQuery()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select count(*) from tabled where field_float8 = :a";
            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Double));

            GetNpDbParam(command.Parameters[0]).Value = 0.123456789012345D;
            
            command.Prepare();

            Object rows = command.ExecuteScalar();

            
            Assert.Equal(1, Convert.ToInt32(rows));
        }
      
        [Fact]
        public void InsertDoubleValue()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tabled(field_float8) values (:a)";
            command.Parameters.Add(new NpgsqlParameter(":a", DbType.Double));

            GetNpDbParam(command.Parameters[0]).Value = 7.4D;

            Int32 rowsAdded = command.ExecuteNonQuery();

            command.CommandText = "select * from tabled where field_float8 = :a";


            var dr = command.ExecuteReader();
            dr.Read();

            Double result = dr.GetDouble(2);


            Assert.Equal(1, Convert.ToInt32(rowsAdded));
            Assert.Equal(7.4D, result);
            
            dr.Close();
        }


        [Fact]
        public void InsertDoubleValueNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tabled(field_float8) values (:a)";
            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Double));

            GetNpDbParam(command.Parameters[0]).Value = 7.4D;

            Int32 rowsAdded = command.ExecuteNonQuery();

            command.CommandText = "select * from tabled where field_float8 = :a";


            var dr = command.ExecuteReader();
            dr.Read();

            Double result = dr.GetDouble(2);


            Assert.Equal(1, Convert.ToInt32(rowsAdded));
            Assert.Equal(7.4D, result);
            
            dr.Close();
        }


        [Fact]
        public void NegativeNumericSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select * from tableb where field_serial = 4";


            var dr = command.ExecuteReader();
            dr.Read();

            Decimal result = dr.GetDecimal(3);

            Assert.Equal(-4.3000000M, result);
            
            dr.Close();
        }


        [Fact]
        public void PrecisionScaleNumericSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select * from tableb where field_serial = 4";


            var dr = command.ExecuteReader();
            dr.Read();

            Decimal result = dr.GetDecimal(3);

            Assert.Equal(-4.3000000M, result);
            //Assert.Equal(11, result.Precision);
            //Assert.Equal(7, result.Scale);
            
            dr.Close();
        }

        [Fact]
        public void InsertNullString()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.String));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tablea where field_text is null";
            command.Parameters.Clear();

            Int64 result = (Int64)command.ExecuteScalar();

            Assert.Equal(4, Convert.ToInt32(result));
        }

        [Fact]
        public void InsertNullStringNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Text));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tablea where field_text is null";
            command.Parameters.Clear();

            Int64 result = (Int64)command.ExecuteScalar();

            Assert.Equal(4, Convert.ToInt32(result));
        }



        [Fact]
        public void InsertNullDateTime()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.DateTime));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tableb where field_timestamp is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar();

            Assert.Equal(4, Convert.ToInt32(result));
        }


        [Fact]
        public void InsertNullDateTimeNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Timestamp));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tableb where field_timestamp is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar();

            Assert.Equal(4, Convert.ToInt32(result));
        }



        [Fact]
        public void InsertNullInt16()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_int2) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int16));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tableb where field_int2 is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(4, Convert.ToInt32(result));
        }


        [Fact]
        public void InsertNullInt16NpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_int2) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Smallint));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tableb where field_int2 is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(4, Convert.ToInt32(result));
        }


        [Fact]
        public void InsertNullInt32()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_int4) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tablea where field_int4 is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(6, Convert.ToInt32(result));
        }


        [Fact]
        public void InsertNullNumeric()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_numeric) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Decimal));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tableb where field_numeric is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(3, Convert.ToInt32(result));
        }

        [Fact]
        public void InsertNullBoolean()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_bool) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Boolean));

            GetNpDbParam(command.Parameters[0]).Value = DBNull.Value;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select count(*) from tablea where field_bool is null";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(6, Convert.ToInt32(result));

        }

        [Fact]
        public void InsertBoolean()
        {
            


            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_bool) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Boolean));

            GetNpDbParam(command.Parameters[0]).Value = false;

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select field_bool from tablea where field_serial = (select max(field_serial) from tablea)";
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(false, result);

        }

        [Fact]
        public void AnsiStringSupport()
        {
         
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:a)";

            command.Parameters.Add(new NpgsqlParameter("a", DbType.AnsiString));

            GetNpDbParam(command.Parameters[0]).Value = "TesteAnsiString";

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = String.Format("select count(*) from tablea where field_text = '{0}'", GetNpDbParam(command.Parameters[0]).Value);
            command.Parameters.Clear();

            Object result = command.ExecuteScalar(); // The missed cast is needed as Server7.2 returns Int32 and Server7.3+ returns Int64

            Assert.Equal(1, Convert.ToInt32(result));
        }


        [Fact]
        public void MultipleQueriesFirstResultsetEmpty()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values ('a'); select count(*) from tablea;";

            Object result = command.ExecuteScalar();

            Assert.Equal(7, Convert.ToInt32(result));
        }

        [Fact]
        public void ConnectionStringWithInvalidParameterValue()
        {

            NpgsqlConnection conn = new NpgsqlConnection(TheConnectionString + ";userid=npgsql_tes;pooling=false");

            NpgsqlCommand command = new NpgsqlCommand("select * from tablea", conn);

            Assert.Throws<NpgsqlException>(() =>
                {

                    try
                    {
                        command.Connection.Open();
                        command.ExecuteReader();
                    }
                    finally
                    {
                        command.Connection.Close();

                    }
                });
            
        }

        [Fact]
        public void InvalidConnectionString()
        {

            Assert.Throws<ArgumentException>(() =>
                {
                    NpgsqlConnection conn = new NpgsqlConnection("Server=127.0.0.1;User Id=npgsql_tests;Pooling:false");

                    conn.Open();
                });
            
        }


        [Fact]
        public void AmbiguousFunctionParameterType()
        {
            //NpgsqlConnection conn = new NpgsqlConnection(TheConnectionString);


            var command = this.TheConnection.CreateCommand();
            command.CommandText = "ambiguousParameterType(:a, :b, :c, :d, :e, :f)";
            command.CommandType = CommandType.StoredProcedure;
            NpgsqlParameter p = new NpgsqlParameter("a", DbType.Int16);
            p.Value = 2;
            command.Parameters.Add(p);
            p = new NpgsqlParameter("b", DbType.Int32);
            p.Value = 2;
            command.Parameters.Add(p);
            p = new NpgsqlParameter("c", DbType.Int64);
            p.Value = 2;
            command.Parameters.Add(p);
            p = new NpgsqlParameter("d", DbType.String);
            p.Value = "a";
            command.Parameters.Add(p);
            p = new NpgsqlParameter("e", NpgsqlDbType.Char);
            p.Value = "a";
            command.Parameters.Add(p);
            p = new NpgsqlParameter("f", NpgsqlDbType.Varchar);
            p.Value = "a";
            command.Parameters.Add(p);

            command.ExecuteScalar();
            
        }
        
        [Fact]
        public void AmbiguousFunctionParameterTypePrepared()
        {
            
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "ambiguousParameterType(:a, :b, :c, :d, :e, :f)";
            command.CommandType = CommandType.StoredProcedure;
            NpgsqlParameter p = new NpgsqlParameter("a", DbType.Int16);
            p.Value = 2;
            command.Parameters.Add(p);
            p = new NpgsqlParameter("b", DbType.Int32);
            p.Value = 2;
            command.Parameters.Add(p);
            p = new NpgsqlParameter("c", DbType.Int64);
            p.Value = 2;
            command.Parameters.Add(p);
            p = new NpgsqlParameter("d", DbType.String);
            p.Value = "a";
            command.Parameters.Add(p);
            p = new NpgsqlParameter("e", DbType.String);
            p.Value = "a";
            command.Parameters.Add(p);
            p = new NpgsqlParameter("f", DbType.String);
            p.Value = "a";
            command.Parameters.Add(p);
    
    
            command.Prepare();
            command.ExecuteScalar();
            
        }


        
        // The following two methods don't need checks because what is being tested is the 
        // execution of parameter replacing which happens on ExecuteNonQuery method. So, if these
        // methods don't throw exception, they are ok.
        [Fact]
        public void TestParameterReplace()
        {
            String sql = @"select * from tablea where
                            field_serial = :a
                         ";


            var command = this.TheConnection.CreateCommand();
            command.CommandText = sql;

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));

            GetNpDbParam(command.Parameters[0]).Value = 2;

            command.ExecuteNonQuery();
        }
        
        [Fact]
        public void TestParameterReplace2()
        {
            String sql = @"select * from tablea where
                         field_serial = :a+1
                         ";


            var command = this.TheConnection.CreateCommand();
            command.CommandText = sql;

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));

            GetNpDbParam(command.Parameters[0]).Value = 1;

            command.ExecuteNonQuery();
        }
        
        [Fact]
        public void TestParameterNameInParameterValue()
        {
            String sql = "insert into tablea(field_text, field_int4) values ( :a, :b );" ;

            String aValue = "test :b";

            var command = this.TheConnection.CreateCommand();
            command.CommandText = sql;

            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Text));

            GetNpDbParam(command.Parameters[":a"]).Value = aValue;
            
            command.Parameters.Add(new NpgsqlParameter(":b", NpgsqlDbType.Integer));

            GetNpDbParam(command.Parameters[":b"]).Value = 1;

            Int32 rowsAdded = command.ExecuteNonQuery();
            Assert.Equal(rowsAdded, 1);
            
            
            var command2 = TheConnection.CreateCommand();
            command2.CommandText = "select field_text, field_int4 from tablea where field_serial = (select max(field_serial) from tablea)";

            var dr = command2.ExecuteReader();
            
            dr.Read();
            
            String a = dr.GetString(0);;
            Int32 b = dr.GetInt32(1);
            
            dr.Close();
            
            
            
            Assert.Equal(aValue, a);
            Assert.Equal(1, b);
        }

        [Fact]
        public void TestBoolParameter1()
        {
            // will throw exception if bool parameter can't be used as boolean expression
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select case when (foo is null) then false else foo end as bar from (select :a as foo) as x";
            NpgsqlParameter p0 = new NpgsqlParameter(":a", true);
            // with setting pramater type
            p0.DbType = DbType.Boolean;
            command.Parameters.Add(p0);

            command.ExecuteScalar();
        }

        [Fact]
        public void TestBoolParameter2()
        {
            // will throw exception if bool parameter can't be used as boolean expression
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select case when (foo is null) then false else foo end as bar from (select :a as foo) as x";
            NpgsqlParameter p0 = new NpgsqlParameter(":a", true);
            // not setting parameter type
            command.Parameters.Add(p0);

            command.ExecuteScalar();
        }

        [Fact]
        public void TestPointSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_point from tablee where field_serial = 1";

            NpgsqlPoint p = (NpgsqlPoint) command.ExecuteScalar();

            Assert.Equal(4, p.X);
            Assert.Equal(3, p.Y);
        }


        [Fact]
        public void TestBoxSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_box from tablee where field_serial = 2";

            NpgsqlBox box = (NpgsqlBox) command.ExecuteScalar();

            Assert.Equal(5, box.UpperRight.X);
            Assert.Equal(4, box.UpperRight.Y);
            Assert.Equal(4, box.LowerLeft.X);
            Assert.Equal(3, box.LowerLeft.Y);
        }

        [Fact]
        public void TestLSegSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_lseg from tablee where field_serial = 3";

            NpgsqlLSeg lseg = (NpgsqlLSeg) command.ExecuteScalar();

            Assert.Equal(4, lseg.Start.X);
            Assert.Equal(3, lseg.Start.Y);
            Assert.Equal(5, lseg.End.X);
            Assert.Equal(4, lseg.End.Y);
        }

        [Fact]
        public void TestClosedPathSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_path from tablee where field_serial = 4";

            NpgsqlPath path = (NpgsqlPath) command.ExecuteScalar();

            Assert.Equal(false, path.Open);
            Assert.Equal(2, path.Count);
            Assert.Equal(4, path[0].X);
            Assert.Equal(3, path[0].Y);
            Assert.Equal(5, path[1].X);
            Assert.Equal(4, path[1].Y);
        }

        [Fact]
        public void TestOpenPathSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_path from tablee where field_serial = 5";

            NpgsqlPath path = (NpgsqlPath) command.ExecuteScalar();

            Assert.Equal(true, path.Open);
            Assert.Equal(2, path.Count);
            Assert.Equal(4, path[0].X);
            Assert.Equal(3, path[0].Y);
            Assert.Equal(5, path[1].X);
            Assert.Equal(4, path[1].Y);
        }



        [Fact]
        public void TestPolygonSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_polygon from tablee where field_serial = 6";

            NpgsqlPolygon polygon = (NpgsqlPolygon) command.ExecuteScalar();

            Assert.Equal(2, polygon.Count);
            Assert.Equal(4, polygon[0].X);
            Assert.Equal(3, polygon[0].Y);
            Assert.Equal(5, polygon[1].X);
            Assert.Equal(4, polygon[1].Y);
        }


        [Fact]
        public void TestCircleSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_circle from tablee where field_serial = 7";

            NpgsqlCircle circle = (NpgsqlCircle) command.ExecuteScalar();

            Assert.Equal(4, circle.Center.X);
            Assert.Equal(3, circle.Center.Y);
            Assert.Equal(5, circle.Radius);
        }
        
        [Fact]
        public void SetParameterValueNull()
        {
            var cmd = this.TheConnection.CreateCommand();
            cmd.CommandText = "insert into tablef(field_bytea) values (:val)";
            NpgsqlParameter param = cmd.CreateParameter() as NpgsqlParameter;
                  param.ParameterName="val";
            param.NpgsqlDbType = NpgsqlDbType.Bytea;
            param.Value = DBNull.Value;
            
            cmd.Parameters.Add(param);
            
            cmd.ExecuteNonQuery();

            cmd = TheConnection.CreateCommand();
            cmd.CommandText = "select field_bytea from tablef where field_serial = (select max(field_serial) from tablef)";
            
            Object result = cmd.ExecuteScalar();
            
            
            Assert.Equal(DBNull.Value, result);
        }
        
        
        [Fact]
        public void TestCharParameterLength()
        {
            String sql = "insert into tableh(field_char5) values ( :a );" ;
    
            String aValue = "atest";
    
            var command = this.TheConnection.CreateCommand();
            command.CommandText = sql;
    
            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Char));
    
            GetNpDbParam(command.Parameters[":a"]).Value = aValue;
            GetNpDbParam(command.Parameters[":a"]).Size = 5;
            
            Int32 rowsAdded = command.ExecuteNonQuery();
            Assert.Equal(rowsAdded, 1);

            var command2 = TheConnection.CreateCommand();
            command2.CommandText = "select field_char5 from tableh where field_serial = (select max(field_serial) from tableh)";

            var dr = command2.ExecuteReader();
            
            dr.Read();
            
            String a = dr.GetString(0);;
                        
            dr.Close();
            
            
            Assert.Equal(aValue, a);
        }
        
        [Fact]
        public void ParameterHandlingOnQueryWithParameterPrefix()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select to_char(field_time, 'HH24:MI') from tablec where field_serial = :a;";
            
            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Integer);
            p.Value = 2;
            
            command.Parameters.Add(p);

            String d = (String)command.ExecuteScalar();


            Assert.Equal("10:03", d);
        }
        
        [Fact]
        public void MultipleRefCursorSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "testmultcurfunc";
            command.CommandType = CommandType.StoredProcedure;

            var dr = command.ExecuteReader();
            
            dr.Read();
            
            Int32 one = dr.GetInt32(0);
            
            dr.NextResult();
            
            dr.Read();
            
            Int32 two = dr.GetInt32(0);
            
            dr.Close();
            
            
            Assert.Equal(1, one);
            Assert.Equal(2, two);
        }
        
        [Fact]
        public void ProcedureNameWithSchemaSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "public.testreturnrecord";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[0]).Direction = ParameterDirection.Output;

            command.Parameters.Add(new NpgsqlParameter(":b", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[1]).Direction = ParameterDirection.Output;

            command.ExecuteNonQuery();
            
            Assert.Equal(4, GetNpDbParam(command.Parameters[0]).Value);
            Assert.Equal(5, GetNpDbParam(command.Parameters[1]).Value);
        }
        
        [Fact]
        public void ReturnRecordSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "testreturnrecord";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[0]).Direction = ParameterDirection.Output;

            command.Parameters.Add(new NpgsqlParameter(":b", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[1]).Direction = ParameterDirection.Output;

            command.ExecuteNonQuery();
            
            Assert.Equal(4, GetNpDbParam(command.Parameters[0]).Value);
            Assert.Equal(5, GetNpDbParam(command.Parameters[1]).Value);
        }
        
        [Fact]
        public void ReturnSetofRecord()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "testreturnsetofrecord";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[0]).Direction = ParameterDirection.Output;

            command.Parameters.Add(new NpgsqlParameter(":b", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[1]).Direction = ParameterDirection.Output;

            command.ExecuteNonQuery();
            
            Assert.Equal(8, GetNpDbParam(command.Parameters[0]).Value);
            Assert.Equal(9, GetNpDbParam(command.Parameters[1]).Value);
        }

        [Fact]
        public void ReturnRecordSupportWithResultset()
        {
            
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "testreturnrecordresultset";
            command.CommandType = CommandType.StoredProcedure;
            
            command.Parameters.Add(new NpgsqlParameter(":a", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[0]).Value = 1;

            command.Parameters.Add(new NpgsqlParameter(":b", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters[1]).Value = 4;


            IDataReader dr = null;
            
            try
            {
                dr = command.ExecuteReader();
            }
            finally
            {
                if (dr != null)
                    dr.Close();
            }
            
        }
        
        [Fact]
        public void ProblemSqlInsideException()
        {
            String sql = "selec 1 as test";
            try
            {
                var command = this.TheConnection.CreateCommand();
            command.CommandText = sql;
                
                command.ExecuteReader();
            }
            catch (NpgsqlException ex)
            {
                Assert.Equal(sql, ex.ErrorSql);
            }
        }

        [Fact]
        public void ReadUncommitedTransactionSupport()
        {
            String sql = "select 1 as test";
            
            using(NpgsqlConnection c = new NpgsqlConnection(TheConnectionString))
            {
            
                c.Open();
                
                NpgsqlTransaction t = c.BeginTransaction(IsolationLevel.ReadUncommitted);
                Assert.NotNull(t);

                var command = TheConnection.CreateCommand();
                command.CommandText = sql;
                    
                command.ExecuteReader().Close();
            }
            
        }
        
        [Fact]
        public void RepeatableReadTransactionSupport()
        {
            String sql = "select 1 as test";
            
            using (NpgsqlConnection c = new NpgsqlConnection(TheConnectionString))
            {
            
                c.Open();
                
                NpgsqlTransaction t = c.BeginTransaction(IsolationLevel.RepeatableRead);
                Assert.NotNull(t);

                var command = TheConnection.CreateCommand();
                command.CommandText = sql;
                    
                command.ExecuteReader().Close();
                
                c.Close();
            }
            
        }
        
        [Fact]
        public void SetTransactionToSerializable()
        {
            String sql = "show transaction_isolation;";
            
            using (NpgsqlConnection c = new NpgsqlConnection(TheConnectionString))
            {
                
            
                c.Open();
                
                NpgsqlTransaction t = c.BeginTransaction(IsolationLevel.Serializable);
                Assert.NotNull(t);
                
                NpgsqlCommand command = new NpgsqlCommand(sql, c);
                
                String isolation = (String)command.ExecuteScalar();
                
                c.Close();
                    
                Assert.Equal("serializable", isolation);
            }
        }
        
        
        [Fact]
        public void TestParameterNameWithDot()
        {
            String sql = "insert into tableh(field_char5) values ( :a.parameter );" ;
    
            String aValue = "atest";
    
            var command = this.TheConnection.CreateCommand();
            command.CommandText = sql;
    
            command.Parameters.Add(new NpgsqlParameter(":a.parameter", NpgsqlDbType.Char));
    
            GetNpDbParam(command.Parameters[":a.parameter"]).Value = aValue;
            GetNpDbParam(command.Parameters[":a.parameter"]).Size = 5;
            
            
            Int32 rowsAdded = -1;
            try
            {
                rowsAdded = command.ExecuteNonQuery();
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine(e.ErrorSql);
            }

            Assert.Equal(rowsAdded, 1);

            var command2 = TheConnection.CreateCommand();
            command2.CommandText = "select field_char5 from tableh where field_serial = (select max(field_serial) from tableh)";
            
            String a = (String)command2.ExecuteScalar();
            
            Assert.Equal(aValue, a);
        }       
        
        [Fact]
        public void DoubleSingleQuotesValueSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_text) values (:a)";
            command.Parameters.Add(new NpgsqlParameter("a", NpgsqlDbType.Text));

            GetNpDbParam(command.Parameters[0]).Value = "''";

            Int32 rowsAdded = command.ExecuteNonQuery();

            Assert.Equal(1, rowsAdded);

            command.CommandText = "select * from tablea where field_text = :a";


            var dr = command.ExecuteReader();
            
            Assert.True(dr.Read());
            
            dr.Close();
        }
        
        [Fact]
        public void ReturnInfinityDateTimeSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values ('infinity'::timestamp);";
            

            command.ExecuteNonQuery();


            command = this.TheConnection.CreateCommand();
            command.CommandText ="select field_timestamp from tableb where field_serial = (select max(field_serial) from tableb);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(DateTime.MaxValue, result);
        }

        [Fact]
        public void ReturnMinusInfinityDateTimeSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values ('-infinity'::timestamp);";
            

            command.ExecuteNonQuery();


            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = (select max(field_serial) from tableb);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(DateTime.MinValue, result);
        }

        [Fact]
        public void InsertInfinityDateTimeSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Timestamp);

            p.Value = DateTime.MaxValue;
            
            command.Parameters.Add(p);

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = (select max(field_serial) from tableb);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(DateTime.MaxValue, result);
        }

        [Fact]
        public void InsertMinusInfinityDateTimeSupportNpgsqlDbType()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Timestamp);

            p.Value = DateTime.MinValue;
            
            command.Parameters.Add(p);

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = (select max(field_serial) from tableb);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(DateTime.MinValue, result);
        }
        
        [Fact]
        public void InsertMinusInfinityDateTimeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tableb(field_timestamp) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", DateTime.MinValue);

            command.Parameters.Add(p);

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_timestamp from tableb where field_serial = (select max(field_serial) from tableb);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(DateTime.MinValue, result);
        }

        [Fact]
        public void MinusInfinityDateTimeSupport()
        {
            var command = TheConnection.CreateCommand();
                       
            command.Parameters.Add(new NpgsqlParameter("p0", DateTime.MinValue));

            command.CommandText = "select 1 where current_date=:p0";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(null, result);
        }
        
        
        [Fact]
        public void PlusInfinityDateTimeSupport()
        {
            var command = TheConnection.CreateCommand();
                       
            command.Parameters.Add(new NpgsqlParameter("p0", DateTime.MaxValue));

            command.CommandText = "select 1 where current_date=:p0";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(null, result);
        }


        [Fact]
        public void InetTypeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablej(field_inet) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Inet);

            p.Value = new NpgsqlInet("127.0.0.1");
            
            command.Parameters.Add(p);

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_inet from tablej where field_serial = (select max(field_serial) from tablej);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal((IPAddress)new NpgsqlInet("127.0.0.1"), (IPAddress)result);
        }

        [Fact]
        public void IPAddressTypeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablej(field_inet) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Inet);

            p.Value = IPAddress.Parse("127.0.0.1");
            
            command.Parameters.Add(p);

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_inet from tablej where field_serial = (select max(field_serial) from tablej);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(IPAddress.Parse("127.0.0.1"), result);
        }

        [Fact]
        public void BitTypeSupportWithPrepare()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablek(field_bit) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Bit);

            p.Value = true;
            
            command.Parameters.Add(p);

            command.Prepare();

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bit from tablek where field_serial = (select max(field_serial) from tablek);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(true, result);
        }

        [Fact]
        public void BitTypeSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablek(field_bit) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Bit);

            p.Value = true;
            
            command.Parameters.Add(p);


            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bit from tablek where field_serial = (select max(field_serial) from tablek);";

            Object result = command.ExecuteScalar();
            
            Assert.Equal(true, result);
        }

        [Fact]
        public void BitTypeSupport2()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablek(field_bit) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Bit);

            p.Value = 3;
            
            command.Parameters.Add(p);


            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bit from tablek where field_serial = (select max(field_serial) from tablek);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(true, result);
        }


        [Fact]
        public void BitTypeSupport3()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "insert into tablek(field_bit) values (:a);";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Bit);

            p.Value = 6;
            
            command.Parameters.Add(p);


            command.ExecuteNonQuery();
            
            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bit from tablek where field_serial = (select max(field_serial) from tablek);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(false, result);
        }

        //[Fact]
        public void FunctionReceiveCharParameter()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "d/;";
            

            NpgsqlParameter p = new NpgsqlParameter("a", NpgsqlDbType.Inet);

            p.Value = IPAddress.Parse("127.0.0.1");
            
            command.Parameters.Add(p);

            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_inet from tablej where field_serial = (select max(field_serial) from tablej);";
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(new NpgsqlInet("127.0.0.1"), result);
        }

        [Fact]
        public void FunctionCaseSensitiveName()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "\"FunctionCaseSensitive\"";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Integer));
            command.Parameters.Add(new NpgsqlParameter("p2", NpgsqlDbType.Text));

            Object result = command.ExecuteScalar();

            Assert.Equal(0, result);
            
        }

        [Fact]
        public void FunctionCaseSensitiveNameWithSchema()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "\"public\".\"FunctionCaseSensitive\"";
            command.CommandType = CommandType.StoredProcedure;
            
            command.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Integer));
            command.Parameters.Add(new NpgsqlParameter("p2", NpgsqlDbType.Text));
            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(0, result);
            
        }
        
        [Fact]
        public void CaseSensitiveParameterNames()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :p1";

            command.Parameters.Add(new NpgsqlParameter("P1", NpgsqlDbType.Integer) { Value = 5 });

            
            Object result = command.ExecuteScalar();
            
            Assert.Equal(5, result);
            
        }


        [Fact]
        public void FunctionTestTimestamptzParameterSupport()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "testtimestamptzparameter";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.TimestampTZ));

            var dr = command.ExecuteReader();

            Int32 count = 0;
            
            while (dr.Read())
                count++;

            Assert.True(count > 1);
            
            
            
            
            
            
        }
        
        [Fact]
        public void GreaterThanInQueryStringWithPrepare()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select count(*) from tablea where field_serial >:param1";
            
            command.Parameters.Add(new NpgsqlParameter(":param1", 1));
            

            command.Prepare();
            command.ExecuteScalar();
            
            
        }
        
        [Fact]
        public void CharParameterValueSupport()
        {
            using (SqlReplicationClient.Sessions.SessionManager.Current.WritableScope())
            {
                const String query = @"create temp table test ( tc char(1) );
            insert into test values(' ');
            select * from test where tc=:charparam";

                var command = this.TheConnection.CreateCommand();
                command.CommandText = query;

                IDbDataParameter sqlParam = command.CreateParameter();
                sqlParam.ParameterName = "charparam";

                // Exception Can't cast System.Char into any valid DbType.
                sqlParam.Value = ' ';
                command.Parameters.Add(sqlParam);

                String res = (String)command.ExecuteScalar();

                Assert.Equal(" ", res);
            }
            
        }
        [Fact]
        public void ConnectionStringCommandTimeout()
        {
           /* NpgsqlConnection connection = new NpgsqlConnection("Server=localhost; Database=test; User=postgres; Password=12345;
CommandTimeout=180");
NpgsqlCommand command = new NpgsqlCommand("\"Foo\"", connection);
connection.Open();*/

	
			
	        using (var conn = this.CreateTimeoutConnection(180))
            {
                
                
		        var command = conn.CreateCommand();
                command.CommandText = "\"Foo\"";
                conn.Open();
                
                Assert.Equal(180, command.CommandTimeout);
            }
            
            
        }
        
         [Fact]
        public void ParameterExplicitType()
        {
            
            object param = 1;
            
            using(var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select a, max(b) from (select :param as a, 1 as b) x group by a";
                cmd.Parameters.Add(new NpgsqlParameter("param", param));
                GetNpDbParam(cmd.Parameters[0]).DbType = DbType.Int32;
                
                using(IDataReader rdr = cmd.ExecuteReader(CommandBehavior.SingleRow))
                {
                    rdr.Read();
                }

                param = "text";
                GetNpDbParam(cmd.Parameters[0]).DbType = DbType.String;
                using(IDataReader rdr = cmd.ExecuteReader(CommandBehavior.SingleRow))
                {
                    rdr.Read();
                }
            
            }
        }
        

        [Fact]
        public void ParameterExplicitType2()
        {
            // there is no execute non query so we must set explicitly that we write.
            using (SqlReplicationClient.Sessions.SessionManager.Current.WritableScope())
            {
                const string query = @"create temp table test ( tc date );  select * from test where tc=:param";

                var command = this.TheConnection.CreateCommand();
                command.CommandText = query;

                IDbDataParameter sqlParam = command.CreateParameter();
                sqlParam.ParameterName = "param";
                sqlParam.Value = "2008-1-1";
                //sqlParam.DbType = DbType.Object;
                command.Parameters.Add(sqlParam);


                command.ExecuteScalar();
            }
        }
        
        [Fact]
        public void ParameterExplicitType2DbTypeObject()
        {
            using (SqlReplicationClient.Sessions.SessionManager.Current.WritableScope())
            {
                const string query = @"create temp table test ( tc date );  select * from test where tc=:param";

                var command = this.TheConnection.CreateCommand();
                command.CommandText = query;

                IDbDataParameter sqlParam = command.CreateParameter();
                sqlParam.ParameterName = "param";
                sqlParam.Value = "2008-1-1";
                sqlParam.DbType = DbType.Object;
                command.Parameters.Add(sqlParam);


                command.ExecuteScalar();
            }
        }
        
        [Fact]
        public void ParameterExplicitType2DbTypeObjectWithPrepare()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "create temp table test ( tc date )";
            command.ExecuteNonQuery();
        
            const string query = @"select * from test where tc=:param";
        
            command = this.TheConnection.CreateCommand();
            command.CommandText = query;

            IDbDataParameter sqlParam = command.CreateParameter();
            sqlParam.ParameterName = "param";
            sqlParam.Value = "2008-1-1";
            sqlParam.DbType = DbType.Object;
            command.Parameters.Add(sqlParam);
           
            command.Prepare();
           
            command.ExecuteScalar();
        }
        
        [Fact]
        public void ParameterExplicitType2DbTypeObjectWithPrepare2()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "create temp table test ( tc date )";
            command.ExecuteNonQuery();
            
            const string query = @"select * from test where tc=:param or tc=:param2";
        
            command = this.TheConnection.CreateCommand();
            command.CommandText = query;

            IDbDataParameter sqlParam = command.CreateParameter();
            sqlParam.ParameterName = "param";
            sqlParam.Value = "2008-1-1";
            sqlParam.DbType = DbType.Object;
            command.Parameters.Add(sqlParam);
            
            sqlParam = command.CreateParameter();
            sqlParam.ParameterName = "param2";
            sqlParam.Value = DateTime.Now;
            sqlParam.DbType = DbType.Date;
            command.Parameters.Add(sqlParam);
            
            command.Prepare();
            
            command.ExecuteScalar();
        }

        [Fact]
        public void Int32WithoutQuotesPolygon()
        {

            var a = this.TheConnection.CreateCommand();
            a.CommandText = "select 'polygon ((:a :b))' ";
            a.Parameters.Add(new NpgsqlParameter("a", 1));
            a.Parameters.Add(new NpgsqlParameter("b", 1));
            
            a.ExecuteScalar();
                      
                 
        }
        
        [Fact]
        public void Int32WithoutQuotesPolygon2()
        {

            var a = this.TheConnection.CreateCommand();
            a.CommandText = "select 'polygon ((:a :b))' ";
            a.Parameters.Add(new NpgsqlParameter("a", 1) { DbType = DbType.Int32 });
            a.Parameters.Add(new NpgsqlParameter("b", 1) { DbType = DbType.Int32 });
            
            a.ExecuteScalar();
                      
                 
        }
        
        [Fact]
        public void TestUUIDDataType()
        {

            string createTable =
            @"DROP TABLE if exists public.person;
            CREATE TABLE public.person ( 
            person_id serial PRIMARY KEY NOT NULL,
            person_uuid uuid NOT NULL
            ) WITH(OIDS=FALSE);";
            var command = this.TheConnection.CreateCommand();
            command.CommandText = createTable;
            command.ExecuteNonQuery();

            string insertSql = "INSERT INTO person (person_uuid) VALUES (:param1);";
            NpgsqlParameter uuidDbParam = new NpgsqlParameter(":param1", NpgsqlDbType.Uuid);
            uuidDbParam.Value = Guid.NewGuid();

            command = this.TheConnection.CreateCommand();
            command.CommandText = insertSql;
            command.Parameters.Add(uuidDbParam);
            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "SELECT person_uuid::uuid FROM person LIMIT 1";


            object result = command.ExecuteScalar();
            Assert.Equal(typeof(Guid), result.GetType());
        }
        
        [Fact]
        public void TestBug1006158OutputParameters()
        {

            string createFunction =
            @"CREATE OR REPLACE FUNCTION more_params(OUT a integer, OUT b boolean) AS
            $BODY$DECLARE
                BEGIN
                    a := 3;
                    b := true;
                END;$BODY$
              LANGUAGE 'plpgsql' VOLATILE;";
              
            var command = this.TheConnection.CreateCommand();
            command.CommandText = createFunction;
            command.ExecuteNonQuery();

            command = this.TheConnection.CreateCommand();
            command.CommandText = "more_params";
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));
            GetNpDbParam(command.Parameters[0]).Direction = ParameterDirection.Output;
            command.Parameters.Add(new NpgsqlParameter("b", DbType.Boolean));
            GetNpDbParam(command.Parameters[1]).Direction = ParameterDirection.Output;

            Object result = command.ExecuteScalar();

            Assert.Equal(3, GetNpDbParam(command.Parameters[0]).Value);
            Assert.Equal(true, GetNpDbParam(command.Parameters[1]).Value);
        }
        
        [Fact]
        public void TestBug1010488ArrayParameterWithNullValue()
        {
            // Test by Christ Akkermans       
            
            var t = TheConnection.CreateCommand();
            t.CommandText = @"CREATE OR REPLACE FUNCTION NullTest (input INT4[]) RETURNS VOID                             
            AS $$
            DECLARE
            BEGIN
            END
            $$ LANGUAGE plpgsql;";
            t.ExecuteNonQuery();

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "NullTest";

                NpgsqlParameter parameter = new NpgsqlParameter("", NpgsqlDbType.Integer | NpgsqlDbType.Array);
                parameter.Value = new object[] { 5, 5, DBNull.Value };
                cmd.Parameters.Add(parameter);
 
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void TestBug1010675ArrayParameterWithNullValue()
        {
            var t = TheConnection.CreateCommand();
            t.CommandText = @"CREATE OR REPLACE FUNCTION NullTest (input INT4[]) RETURNS VOID                             
            AS $$
            DECLARE
            BEGIN
            END
            $$ LANGUAGE plpgsql;";
            t.ExecuteNonQuery();

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "NullTest";

                NpgsqlParameter parameter = new NpgsqlParameter("", NpgsqlDbType.Integer | NpgsqlDbType.Array);
                parameter.Value = new object[] { 5, 5, null };
                cmd.Parameters.Add(parameter);

                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void VarCharArrayHandling()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Varchar | NpgsqlDbType.Array);
                parameter.Value = new object[] { "test", "test"};
                cmd.Parameters.Add(parameter);
 
                cmd.ExecuteNonQuery();
            }
            
            
        }
        
        [Fact]
        public void DoubleArrayHandling()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Double | NpgsqlDbType.Array);
                parameter.Value = new Double[] {1.2d, 1.3d};
                cmd.Parameters.Add(parameter);
 
                cmd.ExecuteNonQuery();
            }
            
            
        }
        
        [Fact]
        public void DoubleArrayHandlingPrepared()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Double | NpgsqlDbType.Array);
                parameter.Value = new Double[] {1.2d, 1.3d};
                cmd.Parameters.Add(parameter);
                
                cmd.Prepare();
 
                cmd.ExecuteNonQuery();
            }
            
            
        }

        
        [Fact]
        public void Bug1010521NpgsqlIntervalShouldBeQuoted()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Interval);
                parameter.Value = new NpgsqlInterval(DateTime.Now.TimeOfDay);
                cmd.Parameters.Add(parameter);
 
                cmd.ExecuteNonQuery();
            }
            
            
        }

        [Fact]
        public void Bug1010543Int32MinValueThrowException()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Integer);
                parameter.Value = Int32.MinValue;
                cmd.Parameters.Add(parameter);

                cmd.ExecuteNonQuery();
            }


        }

        [Fact]
        public void Bug1010543Int16MinValueThrowException()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", DbType.Int16);
                parameter.Value = Int16.MinValue;
                cmd.Parameters.Add(parameter);

                cmd.ExecuteNonQuery();
            }



        }
        [Fact]
        public void Bug1010543Int16MinValueThrowExceptionPrepared()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";

                NpgsqlParameter parameter = new NpgsqlParameter("p1", DbType.Int16);
                parameter.Value = Int16.MinValue;
                cmd.Parameters.Add(parameter);

                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }



        }

        [Fact]
        public void HandleInt16MinValueParameter()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :a";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int16));
            GetNpDbParam(command.Parameters[0]).Value = Int16.MinValue;

            Object result = command.ExecuteScalar();
            Assert.Equal(Int16.MinValue, result);
        }

        [Fact]
        public void HandleInt32MinValueParameter()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :a";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));
            GetNpDbParam(command.Parameters[0]).Value = Int32.MinValue;

            Object result = command.ExecuteScalar();
            Assert.Equal(Int32.MinValue, result);
        }

        [Fact]
        public void HandleInt64MinValueParameter()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :a";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int64));
            GetNpDbParam(command.Parameters[0]).Value = Int64.MinValue;
            
            Object result = command.ExecuteScalar();
            Assert.Equal(Int64.MinValue, result);
        }

        [Fact]
        public void HandleInt16MinValueParameterPrepared()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :a";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int16));
            GetNpDbParam(command.Parameters[0]).Value = Int16.MinValue;
            command.Prepare();

            Object result = command.ExecuteScalar();
            Assert.Equal(Int16.MinValue, result);
        }

        [Fact]
        public void HandleInt32MinValueParameterPrepared()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :a";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int32));
            GetNpDbParam(command.Parameters[0]).Value = Int32.MinValue;
            command.Prepare();

            Object result = command.ExecuteScalar();
            Assert.Equal(Int32.MinValue, result);
        }

        [Fact]
        public void HandleInt64MinValueParameterPrepared()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select :a";
            command.Parameters.Add(new NpgsqlParameter("a", DbType.Int64));
            GetNpDbParam(command.Parameters[0]).Value = Int64.MinValue;
            command.Prepare();

            Object result = command.ExecuteScalar();
            Assert.Equal(Int64.MinValue, result);
        }


        [Fact]
        public void Bug1010557BackslashGetDoubled()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";
                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Text);
                parameter.Value = "test\\str";
                cmd.Parameters.Add(parameter);

                object result = cmd.ExecuteScalar();
                Assert.Equal("test\\str", result); 
            }


        }

        [Fact]
        public void NumberConversionWithCulture()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p1";

                Thread.CurrentThread.CurrentCulture = new CultureInfo("es-ES");

                NpgsqlParameter parameter = new NpgsqlParameter("p1", NpgsqlDbType.Double);
                parameter.Value = 5.5;
                cmd.Parameters.Add(parameter);

                object result = cmd.ExecuteScalar();

                Thread.CurrentThread.CurrentCulture = new CultureInfo("");
                Assert.Equal(5.5, result);

            }


        }
        
        [Fact]
        public void TestNullParameterValueInStatement()
        {
            // Test by Andrus Moor
            IDbCommand cmd = TheConnection.CreateCommand();
            int? i = null;
            cmd.Parameters.Add(new NpgsqlParameter("p0", i));
            cmd.CommandText = "select :p0 is null or :p0=0 ";
            
            cmd.ExecuteScalar();
        }
        
        [Fact]
        public void PreparedStatementWithParametersWithSize()
        {

            using (var cmd = this.TheConnection.CreateCommand())
            {
                cmd.CommandText = "select :p0, :p1;";

                NpgsqlParameter parameter = new NpgsqlParameter("p0", NpgsqlDbType.Varchar);
                parameter.Value = "test";
                parameter.Size = 10;
                cmd.Parameters.Add(parameter);
                
                parameter = new NpgsqlParameter("p1", NpgsqlDbType.Varchar);
                parameter.Value = "test";
                parameter.Size = 10;
                cmd.Parameters.Add(parameter);

                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }



        }
        
        [Fact]
        public void SelectInfinityValueDateDataType()
        {
            // there is no execute non query so we must set explicitly that we write.
            using (SqlReplicationClient.Sessions.SessionManager.Current.WritableScope())
            {
                IDbCommand cmd = TheConnection.CreateCommand();
                cmd.CommandText = "create temp table test (dt date); insert into test values ('-infinity'::date);select * from test";
                var dr = cmd.ExecuteReader();
                dr.Read();
                // InvalidCastException was unhandled
                // at Npgsql.ForwardsOnlyDataReader.GetValue(Int32 Index)
                //  at Npgsql.NpgsqlDataReader.GetDateTime(Int32 i) ..

                dr.GetDateTime(0);

                dr.Close();
            }
        }
       
        [Fact]
        public void Bug1010714AndPatch1010715()
        {
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select field_bytea from tablef where field_bytea = :bytesData";
            
            Byte[] bytes = new Byte[]{45,44};
            
            command.Parameters.Add(new NpgsqlParameter(":bytesData", bytes));
            
            Assert.Equal(DbType.Binary, GetNpDbParam(command.Parameters[0]).DbType);
            
            Object result = command.ExecuteNonQuery();
            

            Assert.Equal(-1, result);
        }
        
        [Fact]
        public void TestNpgsqlSpecificTypesCLRTypesNpgsqlInet()
        {
            // Please, check http://pgfoundry.org/forum/message.php?msg_id=1005483
            // for a discussion where an NpgsqlInet type isn't shown in a datagrid
            // This test tries to check if the type returned is an IPAddress when using
            // the GetValue() of NpgsqlDataReader and NpgsqlInet when using GetProviderValue();
            
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select '192.168.10.10'::inet;";


            var dr = command.ExecuteReader();
            dr.Read();

            Object result = dr.GetValue(0);
            
            dr.Close();
            

            Assert.Equal(typeof(IPAddress), result.GetType());
            
            
        }

        [Fact]
        public void TestNpgsqlSpecificTypesCLRTypesNpgsqlTimeStamp()
        {
            // Please, check http://pgfoundry.org/forum/message.php?msg_id=1005483
            // for a discussion where an NpgsqlInet type isn't shown in a datagrid
            // This test tries to check if the type returned is an IPAddress when using
            // the GetValue() of NpgsqlDataReader and NpgsqlInet when using GetProviderValue();
            
            var command = this.TheConnection.CreateCommand();
            command.CommandText = "select '2010-01-17 15:45'::timestamp;";


            var dr = command.ExecuteReader() as NpgsqlDataReader;
            dr.Read();

            Object result = dr.GetValue(0);
            Object result2 = dr.GetProviderSpecificValue(0);
            
            dr.Close();
            

            Assert.Equal(typeof(DateTime), result.GetType());
            Assert.Equal(typeof(NpgsqlTimeStamp), result2.GetType());
            
            
        }
        
        [Fact]
        public void DataTypeTests()
        {

            // Test all types according to this table:
            // http://www.postgresql.org/docs/9.1/static/datatype.html

            // bigint

            var cmd = this.TheConnection.CreateCommand();
            cmd.CommandText = "select 1::bigint";

            Object result = cmd.ExecuteScalar();

            Assert.Equal(typeof(Int64), result.GetType());

            

            // bit

            cmd.CommandText = "select '1'::bit(1)";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(Boolean), result.GetType());


            // bit(2)

            cmd.CommandText = "select '11'::bit(2)";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(BitString), result.GetType());


            // boolean

            cmd.CommandText = "select 1::boolean";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(Boolean), result.GetType());

            // box

            cmd.CommandText = "select '((7,4),(8,3))'::box";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(NpgsqlBox), result.GetType());

            // bytea 

            cmd.CommandText = @"SELECT E'\\xDEADBEEF'::bytea;";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(Byte[]), result.GetType());


            // varchar(2)

            cmd.CommandText = "select 'aa'::varchar(2);";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(String), result.GetType());

            // char(2)

            cmd.CommandText = "select 'aa'::char(2);";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(String), result.GetType());

            // cidr

            cmd.CommandText = "select '192.168/24'::cidr";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(String), result.GetType());



            // int4

            cmd.CommandText = "select 1::int4";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(Int32), result.GetType());

            // int8

            cmd.CommandText = "select 1::int8";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(Int64), result.GetType());



            // time

            cmd.CommandText = "select current_time::time";

            result = cmd.ExecuteScalar();

            Assert.Equal(typeof(DateTime), result.GetType());
        }


    }
    

    public class CommandTestsV2 : CommandTests
    {
        protected override IDbConnection TheConnection
        {
            get { return _connV2; }
        }
        protected override IDbTransaction TheTransaction {
            get { return _tV2; }
            set { _tV2 = value; }
        }
        protected override string TheConnectionString {
            get { return _connV2String; }
        }
    }
}

