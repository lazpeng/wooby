﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using wooby.Database;
using wooby.Database.Defaults;
using wooby.Error;
using static wooby.Parsing.Parser;

namespace wooby.Parsing;

partial class Parser
{
    public enum TokenKind
    {
        LiteralNumber,
        LiteralString,
        Symbol,
        Keyword,
        Operator,
        Comma,
        SemiColon,
        Dot,
        None
    }

    public class Token
    {
        public TokenKind Kind;
        public string StringValue = string.Empty;
        public double NumberValue;
        public Keyword KeywordValue;
        public Operator OperatorValue;
        public int InputLength { get; init; }
        public string FullText { get; set; } = string.Empty;

        public bool IsOperator()
        {
            return Kind == TokenKind.Operator;
        }

        public bool IsKeyword()
        {
            return Kind == TokenKind.Keyword;
        }
    }

    public struct ExpressionFlags
    {
        // This is for any wildcard, including the syntax table_name.*
        public bool WildcardAllowed = false;

        // This is for the single * character, disallowed when a column has already been specified
        public bool GeneralWildcardAllowed = false;

        // Whether or not you can alias the expression to an identifier
        public bool IdentifierAllowed = false;

        // If a single-value subSelect is allowed
        public bool SingleValueSubSelectAllowed = false;

        // If aggregate functions (e.g. SUM, COUNT) are valid in this context
        public bool AllowAggregateFunctions = false;

        public ExpressionFlags()
        {
        }
    }

    public struct StatementFlags
    {
        // If the select should only return a single column
        public bool SingleValueReturn = false;

        // If the resulting query can stop in a unmatched parenthesis (or throw an error)
        public bool StopOnUnmatchedParenthesis = false;
            
        // Force all output columns to have identifiers
        public bool ForceOutputIdentifiers = false;
            
        // Skip first parenthesis
        public bool SkipFirstParenthesis = false;

        public StatementFlags()
        {
        }
    }

    public struct ReferenceFlags
    {
        public bool WildcardAllowed = false;
        public bool ResolveReferences = false;
        public bool AliasAllowed = true;
        public bool TableOnly = false;

        public ReferenceFlags()
        {
        }
    }
}

public enum Operator
{
    Plus,
    Minus,
    ForwardSlash,
    Asterisk,
    ParenthesisLeft,
    ParenthesisRight,
    Power,
    LessThan,
    MoreThan,
    LessEqual,
    MoreEqual,
    Equal,
    NotEqual,
    Remainder,
    And,
    Or,
}

public enum Keyword
{
    Select,
    From,
    Where,
    Is,
    Not,
    Null,
    True,
    False,
    And,
    Or,
    Asc,
    Desc,
    Order,
    Group,
    By,
    As,
    Create,
    Insert,
    Update,
    Delete,
    Alter,
    Set,
    Into,
    Values,
    Table,
    Column,
    Add,
    Constraint,
    Distinct,
    Left,
    Right,
    Inner,
    Join,
    On,
}

public class Expression
{
    public enum NodeKind
    {
        Operator,
        String,
        Number,
        Reference,
        Function,
        SubSelect,
        Null
    }

    public struct Node
    {
        public NodeKind Kind;
        public string StringValue;
        public double NumberValue;
        public Operator OperatorValue;
        public ColumnReference ReferenceValue;
        public FunctionCall FunctionCall;
        public SelectStatement SubSelect;

        public bool IsWildcard()
        {
            return (Kind == NodeKind.Operator && OperatorValue == Operator.Asterisk) ||
                   (Kind == NodeKind.Reference && ReferenceValue.Column == "*");
        }
    }

    public enum ExpressionType
    {
        Unknown,
        Number,
        String,
        Boolean,
        Null,
        Date
    }

    public static ExpressionType ColumnTypeToExpressionType(ColumnType type)
    {
        return type switch
        {
            ColumnType.Boolean => ExpressionType.Boolean,
            ColumnType.Number => ExpressionType.Number,
            ColumnType.String => ExpressionType.String,
            ColumnType.Date => ExpressionType.Date,
            ColumnType.Null => ExpressionType.Null,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
        };
    }
        
    public static ColumnType ExpressionTypeToColumnType(ExpressionType type)
    {
        return type switch
        {
            ExpressionType.Boolean => ColumnType.Boolean,
            ExpressionType.Number => ColumnType.Number,
            ExpressionType.String => ColumnType.String,
            ExpressionType.Date => ColumnType.Date,
            ExpressionType.Null => ColumnType.Null,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
        };
    }

    public string FullText { get; set; } = string.Empty;
    public string? Identifier { get; set; }
    public List<Node> Nodes { get; init; } = new ();
    public ExpressionType Type { get; set; } = ExpressionType.Unknown;
    public bool IsBoolean { get; set; }
    public bool HasAggregateFunction { get; set; }

    public static Expression WithSingleNode(Node node, ExpressionType type, string fullText)
    {
        return new Expression
        {
            Nodes = new List<Node> {node},
            Type = type,
            FullText = fullText,
            Identifier = "",
        };
    }

    public bool IsWildcard()
    {
        if (Nodes.Count == 0)
        {
            return false;
        }

        return Nodes.Any(n => n.IsWildcard());
    }

    public static bool IsTokenInvalidForExpressionStart(Token token)
    {
        return token.Kind is TokenKind.Keyword or TokenKind.Dot or TokenKind.SemiColon or TokenKind.Comma;
    }

    public bool IsOnlyReference()
    {
        return Nodes is [{ Kind: NodeKind.Reference }];
    }

    public bool IsOnlyFunctionCall()
    {
        return Nodes is [{ Kind: NodeKind.Function }];
    }

    public override bool Equals(object? obj)
    {
        return obj is Expression expression &&
               Nodes.SequenceEqual(expression.Nodes) &&
               Type == expression.Type &&
               IsBoolean == expression.IsBoolean;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(FullText, Identifier, Nodes, Type);
    }
}

public struct FunctionCall
{
    public Function Meta { get; init; }
    public FunctionAccepts CalledVariant { get; set; }
    public List<Expression> Arguments { get; init; }
    private string _fullText = string.Empty;

    public FunctionCall(Function meta, FunctionAccepts calledVariant, List<Expression> arguments)
    {
        Meta = meta;
        CalledVariant = calledVariant;
        Arguments = arguments;
    }

    // Get a value kind of like "Function(a, 2+2, 123, COLUMN)" from this function call
    // for caching purposes
    public string FullText
    {
        get
        {
            if (!string.IsNullOrEmpty(_fullText))
            {
                return _fullText;
            }

            var builder = new StringBuilder();
            foreach (var arg in Arguments)
            {
                if (builder.Length > 0)
                {
                    builder.Append(',');
                }

                builder.Append(arg.FullText);
            }

            _fullText = $"{Meta.Name}({builder})";
            return _fullText;
        }
    }

    public override bool Equals(object? obj)
    {
        return obj is FunctionCall call &&
               Meta.Name == call.Meta.Name &&
               Arguments.SequenceEqual(call.Arguments);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Meta.Name, Arguments);
    }

    public static bool operator ==(FunctionCall left, FunctionCall right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(FunctionCall left, FunctionCall right)
    {
        return !(left == right);
    }
}

public record ColumnReference
{
    public string Table { get; set; } = string.Empty;
    public string Column { get; set; } = string.Empty;
    public string Identifier { get; set; } = string.Empty;
    public int InputLength { get; set; }
    public int ParentLevel { get; init; }
    public Expression.ExpressionType Type { get; set; } = Expression.ExpressionType.Unknown;
    private string _cachedJoin = string.Empty;

    public ColumnReference()
    {
        
    }

    public string Join()
    {
        if (string.IsNullOrEmpty(_cachedJoin))
        {
            // Column is guaranteed to be non empty
            _cachedJoin = string.IsNullOrEmpty(Table) ? Column : $"{Table}.{Column}";
        }
        return _cachedJoin;
    }
}

public enum StatementKind
{
    Query,
    Manipulation,
    Definition
}

public enum StatementClass
{
    Select,
    Insert,
    Update,
    Delete,
    Alter,
    Create,
    Drop,
}

public class TableSource
{
    public enum SourceKind
    {
        Reference,
        SubSelect
    }
        
    public SourceKind Kind { get; init; }
    public ColumnReference Reference { get; init; }
    public SelectStatement? SubSelect { get; init; }
    private TableMeta? CachedMeta { get; set; }

    public string CanonName => string.IsNullOrEmpty(Identifier) ? Table : Identifier;

    public string Identifier
    {
        get
        {
            return Kind switch
            {
                SourceKind.Reference => Reference.Identifier,
                SourceKind.SubSelect => SubSelect?.Identifier ?? "",
                _ => throw new WoobyException("Unreachable code")
            };
        }
    }

    public int InputLength
    {
        get
        {
            return Kind switch
            {
                SourceKind.Reference => Reference.InputLength,
                SourceKind.SubSelect => SubSelect?.InputLength ?? 0,
                _ => throw new WoobyException("Unreachable code")
            };
        }
    }

    public string Table
    {
        get
        {
            return Kind switch
            {
                SourceKind.Reference => Reference.Table,
                SourceKind.SubSelect => Identifier,
                _ => throw new WoobyException("Unreachable code")
            };
        }
    }

    public ColumnMeta? FindReference(ColumnReference reference, Context context)
    {
        if (Kind != SourceKind.Reference) return GetMeta(context).FindColumn(reference);
        
        if (string.IsNullOrEmpty(reference.Table))
        {
            reference.Table = Table;
        }

        return GetMeta(context).FindColumn(reference);
    }

    public TableMeta GetMeta(Context context)
    {
        return Kind switch
        {
            SourceKind.Reference => context.FindTable(Reference) ?? throw new WoobyException("Internal error: Could not find the specified table"),
            SourceKind.SubSelect => BuildMetaFromSubSelect(context),
            _ => throw new WoobyException("Unreachable code")
        };
    }

    public TableMeta BuildMetaFromSubSelect(Context context)
    {
        if (CachedMeta != null)
            return CachedMeta;
            
        if (Kind != SourceKind.SubSelect)
        {
            throw new WoobyException("Internal error: BuildMetaFromSubSelect called when source is not sub select");
        }

        if (SubSelect == null)
        {
            throw new WoobyException("Internal error: Table source is sub select but no value is present");
        }
        
        CachedMeta = new TableMeta
        {
            DataProvider = new InMemoryDataProvider(),
            Id = -1,
            IsReal = false,
            IsTemporary = true,
            Name = SubSelect.Identifier
        };

        foreach (var output in SubSelect.OutputColumns)
        {
            CachedMeta.AddColumn(output.Identifier ?? output.FullText, Expression.ExpressionTypeToColumnType(output.Type));
        }
        CachedMeta.DataProvider.Initialize(context, CachedMeta);
            
        return CachedMeta;
    }

    public bool NameMatches(string reference)
    {
        return (string.IsNullOrEmpty(Identifier) ? Table : Identifier) == reference;
    }
}

public abstract class Statement
{
    public StatementKind Kind { get; protected init; }
    protected StatementClass Class { get; init; }
    public string OriginalText { get; set; } = string.Empty;
    public TableSource MainSource { get; set; } = new();
    public Expression? FilterConditions { get; set; }
    public Statement? Parent { get; set; }
    public StatementFlags UsedFlags { get; set; } = new();
    public int InputLength { get; set; }

    public ColumnReference? TryFindReferenceRecursive(Context context, ColumnReference reference, int level)
    {
        if (reference.Column == "*")
            return reference;
            
        if (MainSource.NameMatches(reference.Table) || reference.Table == "")
        {
            var col = MainSource.FindReference(reference, context);
            if (col != null)
            {
                return new ColumnReference
                {
                    Table = col.Table,
                    ParentLevel = level,
                    Column = col.Name,
                    Type = Expression.ColumnTypeToExpressionType(col.Type)
                };
            }
        }
        else if (this is SelectStatement query)
        {
            var join = query.Joins.FirstOrDefault(j => j.Source.NameMatches(reference.Table));
            var col = join?.Source.FindReference(reference, context);
            if (col != null)
            {
                return new ColumnReference
                {
                    Table = col.Table,
                    ParentLevel = level,
                    Column = col.Name,
                    Type = Expression.ColumnTypeToExpressionType(col.Type)
                };
            }
        }

        return Parent?.TryFindReferenceRecursive(context, reference, level + 1);
    }
}

public enum OrderingKind
{
    Ascending,
    Descending
}

public class Ordering
{
    public Expression OrderExpression { get; init; } = new();
    public OrderingKind Kind { get; set; } = OrderingKind.Ascending;

    public override bool Equals(object? obj)
    {
        return obj is Ordering ordering &&
               OrderExpression.Equals(ordering.OrderExpression) &&
               Kind == ordering.Kind;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(OrderExpression, Kind);
    }
}

public enum JoinKind
{
    Inner,
    Outer,
    Left,
    Right
}

public class Joining
{
    public TableSource Source { get; init; } = new();
    public JoinKind Kind { get; init; } = JoinKind.Inner;
    public Expression Condition { get; init; } = new();
}

public class SelectStatement : Statement
{
    public SelectStatement()
    {
        Kind = StatementKind.Query;
        Class = StatementClass.Select;
    }

    public List<Expression> OutputColumns { get; set; } = new();
    public List<Ordering> OutputOrder { get; init; } = new();
    public List<Expression> Grouping { get; } = new();
    public List<Joining> Joins { get; } = new();
    public string Identifier { get; set; } = string.Empty;
    public bool Distinct { get; set; }

    public override bool Equals(object? obj)
    {
        return obj is SelectStatement statement &&
               Kind == statement.Kind &&
               Class == statement.Class &&
               OutputColumns.SequenceEqual(statement.OutputColumns) &&
               (MainSource == statement.MainSource || MainSource.Equals(statement.MainSource)) &&
               (FilterConditions == statement.FilterConditions ||
                (FilterConditions?.Equals(statement.FilterConditions) ?? false)) &&
               OutputOrder.SequenceEqual(statement.OutputOrder);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Kind, Class, OutputColumns, MainSource, FilterConditions, OutputOrder);
    }
}

public class ColumnNameTypeDef
{
    public string Name { get; init; } = string.Empty;
    public ColumnType Type { get; init; }
}

public class CreateStatement : Statement
{
    public CreateStatement()
    {
        Kind = StatementKind.Definition;
        Class = StatementClass.Create;
    }

    public string Name { get; set; } = string.Empty;
    public List<ColumnNameTypeDef> Columns { get; } = new();
}

public class InsertStatement : Statement
{
    public InsertStatement()
    {
        Kind = StatementKind.Manipulation;
        Class = StatementClass.Insert;
    }

    public List<string> Columns { get; set; } = new();
    public List<Expression> Values { get; set; } = new();
}

public class UpdateStatement : Statement
{
    public UpdateStatement()
    {
        Kind = StatementKind.Manipulation;
        Class = StatementClass.Update;
    }

    public List<Tuple<ColumnReference, Expression>> Columns { get; set; } = new ();
}

public class DeleteStatement : Statement
{
    public DeleteStatement()
    {
        Kind = StatementKind.Manipulation;
        Class = StatementClass.Delete;
    }
}