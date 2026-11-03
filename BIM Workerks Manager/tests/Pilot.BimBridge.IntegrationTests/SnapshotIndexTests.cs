using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdvancedTools.BimBridge;
using NUnit.Framework;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class SnapshotIndexTests
    {
        [Test]public async Task SelectionAndSpatialExistenceShareOneSnapshotScan()
        {
            var provider=new ModelContextProvider();var cache=new SnapshotElementIndexCache(1024*1024,2);
            using(var model=await provider.GetModelAsync(null,null,CancellationToken.None))
            {
                var selected=new List<ElementKey>();
                await new BulkSelectionReader(provider,new BimBridgeSettings(),cache).VisitAsync(model,
                    new BulkSelectionRequest{ModelPartIds=new List<Guid>{provider.Parts[0]}},
                    key=>{selected.Add(key);return Task.CompletedTask;},CancellationToken.None);
                Assert.That(selected.Count,Is.EqualTo(5));Assert.That(provider.Enumerated,Is.EqualTo(5));
                var source=new SpatialSdkSource(model,false,false,false,cache);
                Assert.That(await source.ExistsAsync(selected[0],CancellationToken.None),Is.True);
                Assert.That(await source.ExistsAsync(new ElementKey{ModelPartId=provider.Parts[0],ElementId=Guid.NewGuid()},CancellationToken.None),Is.False);
                Assert.That(provider.Enumerated,Is.EqualTo(5));Assert.That(cache.Builds,Is.EqualTo(1));
                Assert.That(cache.Hits,Is.EqualTo(1));Assert.That(cache.Bytes,Is.GreaterThan(0));
            }
        }
        [Test]public async Task CacheIsVersionScopedAndEvictsLeastRecentlyUsedPart()
        {
            var provider=new ModelContextProvider();var cache=new SnapshotElementIndexCache(1024*1024,1);
            using(var first=await provider.GetModelAsync(null,null,CancellationToken.None))
            using(var second=await provider.GetModelAsync(null,provider.Version.AddMinutes(1),CancellationToken.None))
            {
                var a=await cache.GetAsync(first,provider.Parts[0],CancellationToken.None);
                Assert.That(a.OrderedIds.Length,Is.EqualTo(5));
                Assert.That(await cache.GetAsync(first,provider.Parts[0],CancellationToken.None),Is.SameAs(a));
                await cache.GetAsync(second,provider.Parts[0],CancellationToken.None);
                Assert.That(cache.Entries,Is.EqualTo(1));Assert.That(cache.Builds,Is.EqualTo(2));
                await cache.GetAsync(first,provider.Parts[0],CancellationToken.None);
                Assert.That(cache.Builds,Is.EqualTo(3));Assert.That(cache.Hits,Is.EqualTo(1));
                Assert.That(provider.Enumerated,Is.EqualTo(15));
            }
        }
        [Test]public void OversizedPartFailsInsteadOfStoringUnboundedIds()
        {
            var provider=new ModelContextProvider{ElementsPerPart=1000000};var cache=new SnapshotElementIndexCache(4096,1);
            Assert.ThrowsAsync<BimBridgeException>(async()=>
            {using(var model=await provider.GetModelAsync(null,null,CancellationToken.None))await cache.GetAsync(model,provider.Parts[0],CancellationToken.None);});
            Assert.That(cache.Entries,Is.Zero);Assert.That(cache.Bytes,Is.Zero);
        }
    }
}
