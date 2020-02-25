using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.CodeGenerator.Compatibility;
using Orleans.CodeGenerator.Model;
using Orleans.CodeGenerator.Utilities;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Orleans.CodeGenerator.Generators
{
    internal static class GrainInterfaceCommon
    {
        /// <summary>
        /// Generates switch cases for the provided grain type.
        /// </summary>
        /// <param name="types" />
        /// <param name="grainType" />
        /// <param name="interfaceIdArgument">
        /// The interface id argument, which is used to select the correct switch label.
        /// </param>
        /// <param name="methodIdArgument">
        /// The method id argument, which is used to select the correct switch label.
        /// </param>
        /// <param name="generateMethodHandler">
        /// The function used to generate switch block statements for each method.
        /// </param>
        /// <param name="composeInterfaceBlock">
        /// The function used to compose method switch blocks for each interface.
        /// </param>
        /// <param name="methodDeclaration" />
        public static MethodDeclarationSyntax GenerateGrainInterfaceAndMethodSwitch(
            WellKnownTypes types,
            INamedTypeSymbol grainType,
            ExpressionSyntax interfaceIdArgument,
            ExpressionSyntax methodIdArgument,
            Func<IMethodSymbol, SyntaxList<StatementSyntax>> generateMethodHandler,
            Func<INamedTypeSymbol, StatementSyntax, StatementSyntax, SyntaxList<StatementSyntax>> composeInterfaceBlock,
            MethodDeclarationSyntax methodDeclaration)
        {
            var interfaces = new List<INamedTypeSymbol> { grainType };
            interfaces.AddRange(grainType.AllInterfaces.Where(types.IsGrainInterface));

            var methodDefaultCase = GotoStatement(SyntaxKind.GotoStatement, IdentifierName("MethodNI"));

            // Switch on interface id.
            var interfaceCases = new List<SwitchSectionSyntax>();
            foreach (var type in interfaces)
            {
                var interfaceId = types.GetTypeId(type);
                var typeInterfaces = new[] { type }.Concat(type.AllInterfaces);

                var allMethods = new Dictionary<int, IMethodSymbol>();
                foreach (var typeInterface in typeInterfaces)
                {
                    foreach (var method in typeInterface.GetDeclaredInstanceMembers<IMethodSymbol>())
                    {
                        allMethods[types.GetMethodId(method)] = method;
                    }
                }

                var methodCases = new List<SwitchSectionSyntax>();

                // Switch on method id.
                foreach (var method in allMethods)
                {
                    // Generate the switch label for this method id.
                    var methodIdSwitchLabel = CaseSwitchLabel(method.Key.ToHexLiteral());

                    // Generate the switch body.
                    var methodInvokeStatement = generateMethodHandler(method.Value);

                    methodCases.Add(
                        SwitchSection(SingletonList<SwitchLabelSyntax>(methodIdSwitchLabel), methodInvokeStatement));
                }

                // Generate the switch label for this interface id.
                var interfaceIdSwitchLabel = CaseSwitchLabel(interfaceId.ToHexLiteral());

                // Generate switch statements for the methods in this interface.
                var methodSwitchStatements = methodCases.Count == 0
                    ? SingletonList<StatementSyntax>(methodDefaultCase)
                    : composeInterfaceBlock(type, SwitchStatement(methodIdArgument, List(methodCases)), methodDefaultCase);

                // Generate the switch section for this interface.
                interfaceCases.Add(
                    SwitchSection(SingletonList<SwitchLabelSyntax>(interfaceIdSwitchLabel), methodSwitchStatements));
            }

            var callThrowInterfaceNotImplemented = InvocationExpression(IdentifierName("ThrowInterfaceNotImplemented"))
                .WithArgumentList(ArgumentList(SingletonSeparatedList(Argument(interfaceIdArgument))));

            var callThrowMethodNotImplemented = InvocationExpression(IdentifierName("ThrowMethodNotImplemented"))
                .WithArgumentList(ArgumentList(SeparatedList(new[]
                {
                    Argument(interfaceIdArgument),
                    Argument(methodIdArgument)
                })));

            return methodDeclaration.AddBodyStatements(
                SwitchStatement(interfaceIdArgument, List(interfaceCases)),
                ExpressionStatement(callThrowInterfaceNotImplemented),
                LabeledStatement("MethodNI", ExpressionStatement(callThrowMethodNotImplemented)),
                ThrowStatement(LiteralExpression(SyntaxKind.NullLiteralExpression)),
                interfaceNotImplementedFunction ??= GenerateInterfaceNotImplementedFunction(types),
                methodNotImplementedFunction ??= GenerateMethodNotImplementedFunction(types));
        }

        public static PropertyDeclarationSyntax GenerateInterfaceIdProperty(WellKnownTypes wellKnownTypes, GrainInterfaceDescription description)
        {
            var property = wellKnownTypes.IGrainMethodInvoker.Property("InterfaceId");
            var returnValue = description.InterfaceId.ToHexLiteral();
            return
                PropertyDeclaration(wellKnownTypes.Int32.ToTypeSyntax(), property.Name)
                    .WithExpressionBody(ArrowExpressionClause(returnValue))
                    .AddModifiers(Token(SyntaxKind.PublicKeyword))
                    .WithSemicolonToken(Token(SyntaxKind.SemicolonToken));
        }

        public static PropertyDeclarationSyntax GenerateInterfaceVersionProperty(WellKnownTypes wellKnownTypes, GrainInterfaceDescription description)
        {
            var property = wellKnownTypes.IGrainMethodInvoker.Property("InterfaceVersion");
            var returnValue = LiteralExpression(
                SyntaxKind.NumericLiteralExpression,
                Literal(description.InterfaceVersion));
            return
                PropertyDeclaration(wellKnownTypes.UInt16.ToTypeSyntax(), property.Name)
                    .WithExpressionBody(ArrowExpressionClause(returnValue))
                    .AddModifiers(Token(SyntaxKind.PublicKeyword))
                    .WithSemicolonToken(Token(SyntaxKind.SemicolonToken));
        }

        private static LocalFunctionStatementSyntax interfaceNotImplementedFunction;
        private static LocalFunctionStatementSyntax methodNotImplementedFunction;

        private static LocalFunctionStatementSyntax GenerateInterfaceNotImplementedFunction(
            WellKnownTypes types,
            string interfaceIdVariableName = "i",
            string functionName = "ThrowInterfaceNotImplemented")
        {
            const string interfaceIdText = "InterfaceId: 0x";
            var formatAsHexadecimal = InterpolationFormatClause(Token(SyntaxKind.ColonToken), Token(TriviaList(), SyntaxKind.InterpolatedStringTextToken, "X", "X", TriviaList()));
            var errorMessage = InterpolatedStringExpression(Token(SyntaxKind.InterpolatedStringStartToken))
                .WithContents(List(new InterpolatedStringContentSyntax[]
                {
                    InterpolatedStringText().WithTextToken(Token(TriviaList(), SyntaxKind.InterpolatedStringTextToken, interfaceIdText, interfaceIdText, TriviaList())),
                    Interpolation(IdentifierName(interfaceIdVariableName)).WithFormatClause(formatAsHexadecimal)
                }));
            var throwExpression =
                ThrowExpression(
                    ObjectCreationExpression(types.NotImplementedException.ToTypeSyntax())
                        .AddArgumentListArguments(Argument(errorMessage)));

            var throwInterfaceNotImplemented = LocalFunctionStatement(PredefinedType(Token(SyntaxKind.VoidKeyword)), Identifier(functionName))
                .WithParameterList(ParameterList(SingletonSeparatedList(Parameter(Identifier(interfaceIdVariableName)).WithType(PredefinedType(Token(SyntaxKind.IntKeyword))))))
                .WithExpressionBody(ArrowExpressionClause(throwExpression))
                .WithSemicolonToken(Token(SyntaxKind.SemicolonToken));
            return throwInterfaceNotImplemented;
        }

        private static LocalFunctionStatementSyntax GenerateMethodNotImplementedFunction(
            WellKnownTypes types,
            string interfaceIdVariableName = "i",
            string methodIdVariableName = "m",
            string functionName = "ThrowMethodNotImplemented")
        {
            const string interfaceIdText = "InterfaceId: 0x";
            const string methodIdText = ", MethodId: 0x";

            var formatAsHexadecimal = InterpolationFormatClause(Token(SyntaxKind.ColonToken), Token(TriviaList(), SyntaxKind.InterpolatedStringTextToken, "X", "X", TriviaList()));

            var errorMessage = InterpolatedStringExpression(Token(SyntaxKind.InterpolatedStringStartToken))
                .WithContents(List(new InterpolatedStringContentSyntax[]
                {
                    InterpolatedStringText().WithTextToken(Token(TriviaList(), SyntaxKind.InterpolatedStringTextToken, interfaceIdText, interfaceIdText, TriviaList())),
                    Interpolation(IdentifierName(interfaceIdVariableName)).WithFormatClause(formatAsHexadecimal),
                    InterpolatedStringText().WithTextToken(Token(TriviaList(), SyntaxKind.InterpolatedStringTextToken, methodIdText, methodIdText, TriviaList())),
                    Interpolation(IdentifierName(methodIdVariableName)).WithFormatClause(formatAsHexadecimal),
                }));

            var throwExpression =
                ThrowExpression(
                    ObjectCreationExpression(types.NotImplementedException.ToTypeSyntax())
                        .AddArgumentListArguments(Argument(errorMessage)));

            var throwInterfaceNotImplemented = LocalFunctionStatement(PredefinedType(Token(SyntaxKind.VoidKeyword)), Identifier(functionName))
                .WithParameterList(ParameterList(SeparatedList(new[]
                {
                    Parameter(Identifier(interfaceIdVariableName)).WithType(PredefinedType(Token(SyntaxKind.IntKeyword))),
                    Parameter(Identifier(methodIdVariableName)).WithType(PredefinedType(Token(SyntaxKind.IntKeyword)))
                })))
                .WithExpressionBody(ArrowExpressionClause(throwExpression))
                .WithSemicolonToken(Token(SyntaxKind.SemicolonToken));
            return throwInterfaceNotImplemented;
        }
    }
}
