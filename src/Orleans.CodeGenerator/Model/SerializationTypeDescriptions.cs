using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.CodeGenerator.Compatibility;

namespace Orleans.CodeGenerator.Model
{
    internal class SerializationTypeDescriptions
    {
        public List<SerializerTypeDescription> SerializerTypes { get; } = new List<SerializerTypeDescription>();
        public HashSet<KnownTypeDescription> KnownTypes { get; } = new HashSet<KnownTypeDescription>(KnownTypeDescription.Comparer);
    }

    internal sealed class SerializerTypeDescription
    {
        public bool OverrideExistingSerializer { get; set; }

        private INamedTypeSymbol target;

        public TypeSyntax SerializerTypeSyntax { get; set; }

        public INamedTypeSymbol Target
        {
            get => target;
            set => this.target = value?.OriginalDefinition?.ConstructedFrom;
        }

        public static IEqualityComparer<SerializerTypeDescription> TargetComparer { get; } = new TargetEqualityComparer();

        private sealed class TargetEqualityComparer : IEqualityComparer<SerializerTypeDescription>
        {
            private readonly SymbolEqualityComparer comparer = SymbolEqualityComparer.Default;

            public bool Equals(SerializerTypeDescription x, SerializerTypeDescription y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null) return false;
                if (y is null) return false;
                return this.comparer.Equals(x.Target, y.Target);
            }

            public int GetHashCode(SerializerTypeDescription obj) => this.comparer.GetHashCode(obj.Target);
        }
    }

    internal struct KnownTypeDescription
    {
        public KnownTypeDescription(INamedTypeSymbol type)
        {
            this.Type = type.OriginalDefinition.ConstructedFrom;
        }

        public static IEqualityComparer<KnownTypeDescription> Comparer { get; } = new TypeTypeKeyEqualityComparer();

        public INamedTypeSymbol Type { get; }

        public string TypeKey => this.Type.OrleansTypeKeyString();

        private sealed class TypeTypeKeyEqualityComparer : IEqualityComparer<KnownTypeDescription>
        {
            private readonly SymbolEqualityComparer comparer = SymbolEqualityComparer.Default;

            public bool Equals(KnownTypeDescription x, KnownTypeDescription y) => this.comparer.Equals(x.Type, y.Type);

            public int GetHashCode(KnownTypeDescription obj) => this.comparer.GetHashCode(obj.Type);
        }
    }
}
