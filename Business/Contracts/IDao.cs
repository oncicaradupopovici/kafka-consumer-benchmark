using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace Business.Contracts
{
    public interface IDao
    {
        Task WithConnection(Func<SqlConnection, Task> a);


        Task<bool> CheckIfMessageIsDuplicated(Message msg, SqlTransaction tran = null);

        Task PersistMessageInDedup(Message msg, SqlTransaction tran = null);

        Task PersistMessageAsync(Message msg, SqlTransaction tran = null);

        Task SimulateADatabaseDelayAsync(int miliseconds, SqlTransaction tran = null);
    }
}
