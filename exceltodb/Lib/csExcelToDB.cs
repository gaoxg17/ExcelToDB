﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Serialization;
using System.Xml;
using System.IO;

using log4net;
using log4net.Config;

namespace ExcelToDB.Lib
{

    public class csExcelToDB
    {

        /// <summary>
        /// 私有日志对象
        /// </summary>
        private static readonly ILog logger = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private List<Sheet> _sheets;
        private string _dBType;
        private string _connStr;
        private string _file;

        public List<Sheet> Sheets { get => _sheets; set => _sheets = value; }
        public string ConnStr { get => _connStr; set => _connStr = value; }
        public string File { get => _file; set => _file = value; }
        public string DBType { get => _dBType; set => _dBType = value; }

        #region <xml config>
        /// <summary>
        /// 将对象序列化为XML字符串
        /// </summary>
        /// <returns></returns>
        public string ToXml()
        {

            StringWriter Output = new StringWriter(new StringBuilder());
            string Ret = "";

            try
            {
                XmlSerializer s = new XmlSerializer(this.GetType());
                s.Serialize(Output, this);

                // To cut down on the size of the xml being sent to the database, we'll strip
                // out this extraneous xml.

                Ret = Output.ToString().Replace("xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"", "");
                Ret = Ret.Replace("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"", "");
                Ret = Ret.Replace("<?xml version=\"1.0\" encoding=\"utf-16\"?>", "").Trim();
            }
            catch (Exception ex)
            {
                logger.Error("对象序列化失败！");
                logger.Error("异常描述：\t" + ex.Message);
                logger.Error("异常方法：\t" + ex.TargetSite);
                logger.Error("异常堆栈：\t" + ex.StackTrace);
                throw ex;
            }

            return Ret;
        }

        /// <summary>
        /// 将ＸＭＬ字符串中反序列化为对象
        /// </summary>
        /// <param name="Xml">待反序列化的xml字符串</param>
        /// <returns></returns>
        public csExcelToDB FromXml(string Xml)
        {
            StringReader stringReader = new StringReader(Xml);
            XmlTextReader xmlReader = new XmlTextReader(stringReader);
            csExcelToDB obj;
            try
            {
                XmlSerializer ser = new XmlSerializer(this.GetType());
                obj = (csExcelToDB)ser.Deserialize(xmlReader);
            }
            catch (Exception ex)
            {
                logger.Error("对象反序列化失败！");
                logger.Error("异常描述：\t" + ex.Message);
                logger.Error("异常方法：\t" + ex.TargetSite);
                logger.Error("异常堆栈：\t" + ex.StackTrace);
                throw ex;
            }
            xmlReader.Close();
            stringReader.Close();
            return obj;
        }

        /// <summary>
        /// 从xml文件中反序列化对象
        /// </summary>
        /// <param name="xmlFileName">文件名</param>
        /// <returns>反序列化的对象，失败则返回null</returns>
        public csExcelToDB fromXmlFile(string xmlFileName)
        {
            Stream reader = null;
            csExcelToDB obj = new csExcelToDB();
            try
            {
                XmlSerializer serializer = new XmlSerializer(typeof(csExcelToDB));
                reader = new FileStream(xmlFileName, FileMode.Open);
                obj = (csExcelToDB)serializer.Deserialize(reader);
                reader.Close();
            }
            catch (Exception ex)
            {
                logger.Error("读取配置文件" + xmlFileName + "出现异常！");
                logger.Error("异常描述：\t" + ex.Message);
                logger.Error("引发异常的方法：\t" + ex.TargetSite);
                logger.Error("异常堆栈：\t" + ex.StackTrace);
                obj = null;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }
            }
            return obj;
        }

        /// <summary>
        /// 从xml文件中反序列化对象，文件名默认为：命名空间+类名.config
        /// </summary>
        /// <returns>反序列化的对象，失败则返回null</returns>
        public csExcelToDB fromXmlFile()
        {
            string SettingsFileName = this.GetType().ToString() + ".config";
            return fromXmlFile(SettingsFileName);
        }

        /// <summary>
        /// 将对象序列化到文件中
        /// </summary>
        /// <param name="xmlFileName">文件名</param>
        /// <returns>布尔型。True：序列化成功；False：序列化失败</returns>
        public bool toXmlFile(string xmlFileName)
        {
            Boolean blResult = false;

            if (this != null)
            {
                Type typeOfObj = this.GetType();
                //string SettingsFileName = typeOfObj.ToString() + ".config";

                try
                {
                    XmlSerializer serializer = new XmlSerializer(typeOfObj);
                    TextWriter writer = new StreamWriter(xmlFileName);
                    serializer.Serialize(writer, this);
                    writer.Close();
                    blResult = true;
                }
                catch (Exception ex)
                {
                    logger.Error("保存配置文件" + xmlFileName + "出现异常！");
                    logger.Error("异常描述：\t" + ex.Message);
                    logger.Error("引发异常的方法：\t" + ex.TargetSite);
                    logger.Error("异常堆栈：\t" + ex.StackTrace);
                }
                finally
                {
                }
            }
            return blResult;
        }

        /// <summary>
        /// 将对象序列化到文件中，文件名默认为：命名空间+类名.config
        /// </summary>
        /// <returns>布尔型。True：序列化成功；False：序列化失败</returns>
        public bool toXmlFile()
        {
            string SettingsFileName = this.GetType().ToString() + ".config";
            return toXmlFile(SettingsFileName);
        }
        #endregion
    }
}
