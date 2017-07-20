using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Business.Contracts;

namespace Business
{
    public class DeduplicationDecorator : IMessageHandler
    {
        private readonly IMessageHandler _innerHandler;
        private readonly IDao _dao;
        

        public DeduplicationDecorator(IMessageHandler innerHandler, IDao dao)
        {
            _innerHandler = innerHandler;
            _dao = dao;
        }
        public async Task HandleAsync(Message msg, SqlTransaction tran = null)
        {

            var msgAllreadyProcessed = await _dao.CheckIfMessageIsDuplicated(msg, tran);
            if (msgAllreadyProcessed)
                return;

            if (tran != null)
            {
                await InternalHandleAsync(msg, tran);
            }
            else
            {
                await _dao.WithConnection(async cn =>
                {
                    var localTran = cn.BeginTransaction();
                    try
                    {
                        await InternalHandleAsync(msg, localTran);
                        localTran.Commit();
                    }
                    catch (Exception ex)
                    {
                        localTran.Rollback();
                    }
                });
            }



        }

        private async Task InternalHandleAsync(Message msg, SqlTransaction tran)
        {
            await _innerHandler.HandleAsync(msg, tran);
            await _dao.PersistMessageInDedup(msg, tran);
        }


    }
}
