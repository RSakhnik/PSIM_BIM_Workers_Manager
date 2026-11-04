using System;
using System.Linq;
using System.Security.Cryptography;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    // Transport checks only. Worker-specific schemas belong to clients.
    internal static class DiagnosticTransport
    {
        internal const int MaxPayloadBytes = 64 * 1024;

        internal static DiagnosticEnvelopeDto Accept(DiagnosticEnvelopeDto value)
        {
            if (value == null) return null;
            if (value.SchemaVersion < 1 || value.PayloadBytes == null ||
                value.PayloadBytes.Length == 0 || value.PayloadBytes.Length > MaxPayloadBytes ||
                string.IsNullOrWhiteSpace(value.ContentType) || value.ContentType.Length > 128 ||
                value.ContentType.Any(ch => ch < 32 || ch > 126) ||
                value.Sha256 == null || value.Sha256.Length != 64 || !value.Sha256.All(Uri.IsHexDigit))
                return null;
            using (var sha = SHA256.Create())
            {
                var digest = BitConverter.ToString(sha.ComputeHash(value.PayloadBytes)).Replace("-", "");
                if (!string.Equals(digest, value.Sha256, StringComparison.OrdinalIgnoreCase)) return null;
            }
            return new DiagnosticEnvelopeDto {
                ContentType=value.ContentType, SchemaVersion=value.SchemaVersion,
                PayloadBytes=(byte[])value.PayloadBytes.Clone(), Sha256=value.Sha256.ToUpperInvariant() };
        }

        internal static bool Same(DiagnosticEnvelopeDto left, DiagnosticEnvelopeDto right) =>
            left?.ContentType == right?.ContentType && left?.SchemaVersion == right?.SchemaVersion &&
            string.Equals(left?.Sha256, right?.Sha256, StringComparison.OrdinalIgnoreCase) &&
            (left?.PayloadBytes ?? Array.Empty<byte>()).SequenceEqual(right?.PayloadBytes ?? Array.Empty<byte>());
    }
}
