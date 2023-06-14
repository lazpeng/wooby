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
        public List<ColumnValue> Columns { get; set; } = new List<ColumnValue>();
    }

    public class JsonCustomData
    {
        public int Version { get; set; } = 1;
        public DateTime CreationDate { get; set; } = DateTime.Now;
        public Dictionary<long, Dictionary<long, JsonTableRow>> Data { get; set; } = new Dictionary<long, Dictionary<long, JsonTableRow>>();
        public long NextRowId { get; set; } = 0;
    }
}
