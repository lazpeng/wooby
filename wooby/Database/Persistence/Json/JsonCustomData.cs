using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence.Json
{
    public class JsonTableRow
    {
        public long RowId { get; set; }
        public List<ColumnValue> Columns { get; set; }
    }

    public class JsonCustomData
    {
        public int Version { get; set; } = 1;
        public DateTime CreationDate { get; set; } = DateTime.Now;
        public Dictionary<long, List<JsonTableRow>> Data { get; set; }
    }
}
