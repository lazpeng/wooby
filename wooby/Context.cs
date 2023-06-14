using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
    public enum ColumnType
    {
        String,
        Number,
        Boolean
    }

    public class ColumnMeta
    {
        public string Name { get; set; }
        public int Id { get; set; }
        public bool Unique { get; set; }
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
        public Schema Parent { get; set; }
    }

    public class Schema
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public bool IsMain { get; set; }
        public List<TableMeta> Tables { get; set; } = new List<TableMeta>();
    }

    public class GlobalVariable
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public ColumnType Type { get; set; }
    }

    public class Context
    {
        public List<Schema> Schemas { get; private set; } = new List<Schema>();
        public List<GlobalVariable> Variables { get; private set; } = new List<GlobalVariable>();

        public Context()
        {
            AddSchema(new Schema() { IsMain = true, Name = "main" });
        }

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

        public static void AddTable(TableMeta table, Schema target)
        {
            table.Parent = target;

            long id = 0;
            if (target.Tables.Count > 0)
            {
                id = target.Tables.Max(t => t.Id) + 1;
            }
            table.Id = id;
            target.Tables.Add(table);
        }

        public void AddSchema(Schema schema)
        {
            int id = 0;
            if (Schemas.Count > 0)
            {
                id = Schemas.Max(s => s.Id) + 1;
            }

            schema.Id = id;
            Schemas.Add(schema);
        }

        public TableMeta FindTable(TableReference reference)
        {
            string schema = reference.Schema;
            if (string.IsNullOrEmpty(schema))
            {
                schema = "main";
            }

            var schemaMeta = Schemas.Find(s => s.Name.ToUpper() == schema.ToUpper());
            if (schemaMeta != null)
            {
                return schemaMeta.Tables.Find(t => t.Name.ToUpper() == reference.Table.ToUpper());
            }

            return null;
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

        public GlobalVariable FindVariable(string Name)
        {
            return Variables.Find(v => v.Name.ToUpper() == Name.ToUpper());
        }

        public bool IsReferenceValid(ColumnReference reference)
        {
            return FindColumn(reference) != null || (string.IsNullOrEmpty(reference.Table) && FindVariable(reference.Column) != null);
        }

        public bool IsReferenceValid(TableReference reference)
        {
            return FindTable(reference) != null;
        }
    }
}
