using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LocalPIData
{
    /// <summary>
    /// PI History Data
    /// </summary>
    public class PIHisData
    {
        public static string ip = (string)(new System.Configuration.AppSettingsReader()).GetValue("ip", typeof(string));
        public static string user = (string)(new System.Configuration.AppSettingsReader()).GetValue("username", typeof(string));
        public static string psd = (string)(new System.Configuration.AppSettingsReader()).GetValue("password", typeof(string));

        /// <summary>
        /// constructor
        /// </summary>
        public PIHisData()
        {
            new PI.PIFunc2(ip, user, psd);
        }

        /// <summary>
        /// Get PI History Value
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <returns></returns>
        public double? GetHisValue(string pn, DateTime ts)
        {
            PI.RetVal rv = (new PI.PIFunc2(ip, user, psd)).GetPointHisValue(pn, ts);
            return rv == null ? (double?)null : rv.pvalue;
        }
    }
}
