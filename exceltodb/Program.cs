using System;
using log4net;
using ExcelToDB.Lib;

namespace ExcelToDB
{
    class Program
    {
        private static readonly ILog logger = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static void Main(string[] args)
        {
            Console.WriteLine($"开始执行 {DateTime.Now:F}\n");
            logger.Info("开始执行");

            var eo = new csExcelToDB().fromXmlFile();

            //check config
            var checkConfig = Exec.CheckConfig(eo);
            if (!checkConfig.success)
            {
                Console.WriteLine(checkConfig.errMsg);
                logger.Info(checkConfig.errMsg);
                goto end;
            }
            //to db                  
            var toDB = Exec.ToDB(eo);
            if (!toDB.success)
            {
                Console.WriteLine(toDB.errMsg);
                logger.Info(toDB.errMsg);
                goto end;
            }
            else
            {
                var execMsg = toDB.errMsg;
                Console.WriteLine(execMsg);
                logger.Info(execMsg);
            }

            Console.WriteLine($"本次执行完成\n");
            logger.Info($"本次执行完成\n");

            end: Console.WriteLine("Please enter any key to colse..");
            Console.ReadKey(true);
        }
    }
}
