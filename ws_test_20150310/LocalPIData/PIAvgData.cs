using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LocalPIData
{
    /// <summary>
    /// PI Average Value
    /// </summary>
    public class PIAvgData
    {
        /// <summary>
        /// 构造函数
        /// </summary>
        public PIAvgData()
        {
            new PI.PIFunc2("10.150.124.193", "pirw", "pirw");
        }

        /// <summary>
        /// Get Average Value
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="shift"></param>
        /// <returns></returns>
        public double? GetAvgValue(string pn, DateTime st, DateTime et, int shift)
        {
            return (new PI.PIFunc2("10.150.124.193", "pirw", "pirw")).GetAverageValue(pn, st.AddSeconds(shift), et.AddSeconds(shift));         
        }
    }
}
