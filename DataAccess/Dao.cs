using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using Business;
using Business.Contracts;

namespace DataAccess
{
    public class Dao : IDao
    {

        private static int _connectionCount = 0;
        private readonly HashSet<string> _dedupTables = new HashSet<string>();
        private int _consumerTableCreated = 0;


        public async Task WithConnection(Func<SqlConnection,Task> a)
        {
            var cnx = await GetNewConnectionAsync();
            try
            {
                await a(cnx);
            }
            finally
            {
                ReleaseConnection(cnx);
            }
        }

        

        

        public async Task<bool> CheckIfMessageIsDuplicated(Message msg, SqlTransaction tran = null)
        {
            var tableName = GetDedupTableName(msg.ConsumerGroup, msg.Topic, msg.Partition);
            if (!_dedupTables.Contains(tableName))
            {
                    await EnsureConsumerTopicPartitionDedupTableCreated(msg.ConsumerGroup, msg.Topic, msg.Partition,
                        tran);

                _dedupTables.Add(tableName);
                return false;
            }

            var sql =
                "select cast(1 as bit)" + Environment.NewLine +
                "where exists(" + Environment.NewLine +
                "select 1" + Environment.NewLine +
                $"from {tableName}" + Environment.NewLine +
                $"where MsgUniqueIdentifier = {msg.Value}" + Environment.NewLine +
                ")";

            var isDuplicated = await ExecuteScalarAsync(sql, tran);
            return isDuplicated is bool && (bool)isDuplicated;
        }

        public Task PersistMessageInDedup(Message msg, SqlTransaction tran = null)
        {
            var tableName = GetDedupTableName(msg.ConsumerGroup, msg.Topic, msg.Partition);
            var sql =
                $"insert into {tableName}(MsgUniqueIdentifier)" + Environment.NewLine +
                $"select {msg.Value}";
               
            return ExecuteNonQueryAsync(sql, tran);
        }

        public async Task PersistMessageAsync(Message msg, SqlTransaction tran = null)
        {
            var tableName = GetConsumerTableName(msg.ConsumerGroup);
            if (_consumerTableCreated == 0)
            {
                await EnsureConsumerTableCreated(msg.ConsumerGroup, tran);
                Interlocked.Increment(ref _consumerTableCreated);
            }

            var sql =
                $"insert into {tableName}([Key], Value, Topic, Partition, Offset, ConsumerId) " +
                $"select '{msg.Key}', '{msg.Value}', '{msg.Topic}', {msg.Partition}, {msg.Offset}, '{msg.ConsumerId}'";

            await ExecuteNonQueryAsync(sql, tran);
        }

        public Task SimulateADatabaseDelayAsync(int miliseconds, SqlTransaction tran = null)
        {
            var sql =$"WAITFOR DELAY '00:00:00.{miliseconds}'";

            return ExecuteNonQueryAsync(sql, tran);
        }




        private async Task EnsureConsumerTopicPartitionDedupTableCreated(string consumerGroup, string topic, int partition, SqlTransaction tran = null)
        {
            var tableName = GetDedupTableName(consumerGroup, topic, partition);

            var sql =
                $"begin tran" + Environment.NewLine +
                $"if not exists (select 1 from sys.tables where name='{tableName}')" + Environment.NewLine +
                $"CREATE TABLE {tableName}(" + Environment.NewLine +
                @"[MsgUniqueIdentifier] [int] NOT NULL primary key,
	            [RecordStamp] [datetime] NOT NULL default(getdate()))" + Environment.NewLine +
                "commit";

            try
            {
                await ExecuteNonQueryAsync(sql, tran);
            }
            catch
            {
                //concurency issue
            }
        }

        private async Task EnsureConsumerTableCreated(string consumerGroup, SqlTransaction tran = null)
        {
            var tableName = GetConsumerTableName(consumerGroup);
            var sql =
                $"begin tran" + Environment.NewLine +
                $"if not exists (select 1 from sys.tables where name='{tableName}')" + Environment.NewLine +
                $"CREATE TABLE {consumerGroup}(" + Environment.NewLine +
                @"[Id] [int] IDENTITY(1,1) NOT NULL primary key,
	            [Key] [varchar](100) NULL,
	            [Value] [varchar](1000) NOT NULL,
	            [Topic] [varchar](100) NOT NULL,
	            [Partition] [int] NOT NULL,
	            [Offset] [bigint] NOT NULL,
	            [ConsumerId] [varchar](50) NOT NULL,
	            [RecordStamp] [datetime] NOT NULL default(getdate()))" + Environment.NewLine +
                "commit";

            try
            {
                await ExecuteNonQueryAsync(sql, tran);
            }
            catch
            {
                //concurency issue
            }
        }

        private Task ExecuteNonQueryAsync(string sql, SqlTransaction tran = null)
        {
            var externalProvidedTransaction = tran != null;

            return externalProvidedTransaction
                ? InternalExecuteNonQueryAsync(sql, tran.Connection, tran) 
                : WithConnection(cn => InternalExecuteNonQueryAsync(sql, cn, null));
        }
        private async Task<object> ExecuteScalarAsync(string sql, SqlTransaction tran = null)
        {
            var externalProvidedTransaction = tran != null;

            object result = null;

            if (externalProvidedTransaction)
                result = await InternalExecuteScalarAsync(sql, tran.Connection, tran);
            else
                await WithConnection(async cn =>
                {
                    result = await InternalExecuteScalarAsync(sql, cn, null);
                });

            return result;
        }

        private Task InternalExecuteNonQueryAsync(string sql, SqlConnection cnx, SqlTransaction tran)
        {
            var cmd = cnx.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tran;
            return cmd.ExecuteNonQueryAsync();
        }

        private Task<object> InternalExecuteScalarAsync(string sql, SqlConnection cnx, SqlTransaction tran)
        {
            var cmd = cnx.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tran;
            return cmd.ExecuteScalarAsync();
        }

        private async Task<SqlConnection> GetNewConnectionAsync()
        {
            SqlConnection cnx = null;

            var success = false;
            var retries = 0;
            while (!success)
            {
                try
                {
                    //cnx = new SqlConnection("Server=.;Database=Kafka;Trusted_Connection=True;MultipleActiveResultSets=true;");
                    cnx = new SqlConnection("Server=.;Database=Kafka;Trusted_Connection=True;");
                    await cnx.OpenAsync();
                    success = true;
                }
                catch(Exception ex)
                {
                    if (retries > 20)
                        throw;

                    success = false;
                    retries++;
                    await Task.Delay(100);
                }
            }

            Interlocked.Increment(ref _connectionCount);

            return cnx;
        }

        private void ReleaseConnection(SqlConnection cnx)
        {
            cnx.Close();
            Interlocked.Decrement(ref _connectionCount);
        }

        private string GetDedupTableName(string consumerGroup, string topic, int partition)
        {
            var tableName = $"DEDUP__{consumerGroup}__{topic.Replace("-", "_")}__{partition}";
            return tableName;
        }

        private string GetConsumerTableName(string consumerGroup)
        {
            var tableName = $"{consumerGroup}";
            return tableName;
        }

    }
}
