using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public TableMeta Parent { get; set; }
    }

    public class TableMeta
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public List<ColumnMeta> Columns { get; set; } = new List<ColumnMeta>();
        public bool IsReal { get; set; }
        public bool IsTemporary { get; set; }
    }

    public class Function
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public ColumnType Type { get; set; }
        public List<ColumnType> Parameters { get; set; }
        public bool IsAggregate { get; set; }
    }

    public class Context
    {
        public List<TableMeta> Tables { get; set; } = new List<TableMeta>();
        public List<Function> Functions { get; private set; } = new List<Function>();

        public static void AddColumn(ColumnMeta column, TableMeta target)
        {
            column.Parent = target;

            int id = 0;
            if (target.Columns.Count > 0)
            {
                id = target.Columns.Max(c => c.Id) + 1;
            }
            column.Id = id;
            target.Columns.Add(column);
        }

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
