using System.Collections.Generic;
using System.Linq;

using wooby.Database;
using wooby.Parsing;

namespace wooby
{
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
        public ColumnFlags Flags { get; set; } = new ColumnFlags();
        public ColumnType Type { get; set; }
        public long Parent { get; set; }
    }

    public class TableMeta
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public List<ColumnMeta> Columns { get; set; } = new List<ColumnMeta>();
        public bool IsReal { get; set; }
        public bool IsTemporary { get; set; }

        public TableMeta AddColumn(string ColumnName, ColumnType Type, ColumnFlags flags = null)
        {
            var col = new ColumnMeta { Id = Columns.Count, Name = ColumnName, Parent = this.Id, Type = Type, Flags = flags ?? new ColumnFlags() };
            Columns.Add(col);
            return this;
        }
    }

    public class Context
    {
        public List<TableMeta> Tables { get; set; } = new List<TableMeta>();
        public List<Function> Functions { get; private set; } = new ();
        public string DatabaseFilename { get; set; }
        public ContextSourceType DatabaseSource { get; set; }
        public object CustomSourceData { get; set; }
        public string CustomSourceDataString { get; set; }

        public void AddTable(TableMeta table)
        {
            long id = 0;
            if (Tables.Count > 0)
            {
                id = Tables.Max(t => t.Id) + 1;
            }
            table.Id = id;
            Tables.Add(table);
        }

        public void AddFunction(Function v)
        {
            Functions.Add(v);
        }

        public TableMeta FindTable(ColumnReference reference)
        {
            return Tables.Find(t => t.Name.ToUpper() == reference.Table.ToUpper());
        }

        public ColumnMeta FindColumn(ColumnReference reference)
        {
            var tableMeta = FindTable(reference);

            if (tableMeta != null)
            {
                return tableMeta.Columns.Find(c => c.Name.ToUpper() == reference.Column.ToUpper());
            }

            return null;
        }

        public Function FindFunction(string Name)
        {
            return Functions.Find(v => v.Name.ToUpper() == Name.ToUpper());
        }
    }
}
