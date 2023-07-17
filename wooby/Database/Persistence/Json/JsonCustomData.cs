using System;
using System.Collections.Generic;

namespace wooby.Database.Persistence.Json;

public class JsonTableRow
{
    public long RowId { get; init; }
    public List<BaseValue> Columns { get; init; } = new ();
}

public class JsonCustomData
{
    public int Version { get; set; } = 1;
    public DateTime CreationDate { get; set; } = DateTime.Now;
    public Dictionary<long, Dictionary<long, JsonTableRow>> Data { get; set; } = new Dictionary<long, Dictionary<long, JsonTableRow>>();
    public long NextRowId { get; set; }
}