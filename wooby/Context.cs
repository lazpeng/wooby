using System;
using System.Collections.Generic;
using System.Linq;

using wooby.Database;
using wooby.Database.Persistence;
using wooby.Parsing;

namespace wooby;

public enum ColumnType
{
    String,
    Number,
    Boolean,
    Date,
    Null
}

public enum ContextSourceType
{
    Json
}

public class ColumnFlags
{
    public bool Unique = false;
    public bool Nullable = true;
    public bool PrimaryKey = false;
}

public class ColumnMeta
{
    public string Name { get; set; }
    public int Id { get; set; }
    public ColumnFlags Flags { get; set; } = new ();
    public ColumnType Type { get; set; }
    public string Table { get; set; }
    public long Parent { get; set; }
}

public class TableMeta
{
    public string Name { get; set; }
    public long Id { get; set; }
    public List<ColumnMeta> Columns { get; set; } = new ();
    public bool IsReal { get; set; }
    public bool IsTemporary { get; set; }
    public ITableDataProvider DataProvider { get; set; }

    public TableMeta AddColumn(string columnName, ColumnType type, ColumnFlags flags = null)
    {
        var col = new ColumnMeta { Id = Columns.Count, Name = columnName, Parent = Id, Type = type, Flags = flags ?? new ColumnFlags(), Table = Name };
        Columns.Add(col);
        return this;
    }

    public ColumnMeta FindColumn(ColumnReference reference)
    {
        return Columns.Find(c => c.Name.ToUpper() == reference.Column.ToUpper());
    }
}

public class Context
{
    private readonly List<Function> _functions = new ();
    private readonly List<TableMeta> _tables = new ();
        
    public IReadOnlyList<Function> Functions => _functions;
    public IReadOnlyList<TableMeta> Tables => _tables;
    public string DatabaseFilename { get; set; }
    public ContextSourceType DatabaseSource { get; set; }
    public object CustomSourceData { get; set; }
    public string CustomSourceDataString { get; set; }

    public void AddTable(TableMeta table)
    {
        if (Tables.Any(t => t.Name == table.Name))
        {
            throw new Exception("Duplicate table");
        }

        table.DataProvider ??= PersistenceBackendHelper.GetTableDataProvider(this);
        table.DataProvider.Initialize(this, table);
            
        long id = 0;
        if (Tables.Count > 0)
        {
            id = Tables.Max(t => t.Id) + 1;
        }
        table.Id = id;
        _tables.Add(table);
    }

    public void ResetNotReal()
    {
        _tables.RemoveAll(t => !t.IsReal);
    }

    public void AddFunction(Function v)
    {
        _functions.Add(v);
    }

    public TableMeta FindTable(ColumnReference reference)
    {
        return _tables.Find(t => string.Equals(t.Name, reference.Table, StringComparison.CurrentCultureIgnoreCase));
    }

    public TableMeta FindTable(long id)
    {
        return _tables.Find(t => t.Id == id);
    }

    public ColumnMeta FindColumn(ColumnReference reference)
    {
        var tableMeta = FindTable(reference);

        return tableMeta?.FindColumn(reference);
    }

    public Function FindFunction(string name)
    {
        return _functions.Find(v => string.Equals(v.Name, name, StringComparison.CurrentCultureIgnoreCase));
    }
}