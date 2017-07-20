using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace Business.Contracts
{
    public interface IMessageHandler
    {
        Task HandleAsync(Message msg, SqlTransaction tran = null);

    }
}
