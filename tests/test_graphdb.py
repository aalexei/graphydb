import unittest
import random

import os,sys
parentdir = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0,parentdir)

from graphydb import Graph, NSet, ESet, Node, Edge, GraphyDBException


class SimpleTests(unittest.TestCase):
    
    def setUp(s):
        s.gt = Graph(":memory:")
        s.gt.reset()
        
    def test_settings(s):
        hello =  [1,2,3,4,'hello']
        s.gt.savesetting('test', hello)
        s.assertEqual( s.gt.getsetting('test'), hello)
        
    def test_cache(s):
        hello =  [1,2,3,4,'hello']
        s.gt.cache('test', hello)
        s.assertEqual( s.gt.cached('test'), hello)

class FriendGraphTests(unittest.TestCase):
    
    def setUp(s):
        s.gt = Graph(":memory:") 
        
        s.p1 = s.gt.Node('Person', name='Anne').save()
        s.p2 = s.gt.Node('Person', name='Bob').save()
        s.p3 = s.gt.Node('Person', name='Charlotte').save()
        s.p4 = s.gt.Node('Person', name='Dirk').save()
        s.p5 = s.gt.Node('Person', name='Eugene').save()
        s.p6 = s.gt.Node('Person', name='Fred').save()

        s.e1 = s.gt.Edge(s.p1, 'Likes', s.p2).save()
        s.e2 = s.gt.Edge(s.p2, 'Likes', s.p3).save()
        s.e3 = s.gt.Edge(s.p2, 'Likes', s.p1).save()
        s.e4 = s.gt.Edge(s.p4, 'Likes', s.p5).save()
        s.e5 = s.gt.Edge(s.p4, 'Likes', s.p6).save()

        s.e6 = s.gt.Edge(s.p1, 'Follows', s.p5).save()
        s.e7 = s.gt.Edge(s.p2, 'Follows', s.p6).save()
        s.e8 = s.gt.Edge(s.p6, 'Follows', s.p3).save()
        
        s.e9 = s.gt.Edge(s.p3, 'Likes', s.p5).save()
        s.e10 = s.gt.Edge(s.p1, 'Follows', s.p2).save()

            
        s.gt.resetfts(nodefields=['name'])
        
        for item in [s.p1,s.p2,s.p3,s.p4,s.p5,s.p6]:
            item.updatefts(name=item['name'])        
            
    def test_bothN(s):
        s.assertEqual(
            set([n['name'] for n in s.p2.bothN().bothN().bothN()]), 
            set(['Fred', 'Charlotte', 'Anne', 'Bob', 'Dirk', 'Eugene'])
        )
        
    def test_GLOB(s):
        s.assertEqual(
            s.gt.fetch(CHAIN='(n)', WHERE='n.uid GLOB :a', a=s.p1['uid'][:5]+'*').one['name'], 'Anne'
            )
    
    def test_end(s):
        s.assertEqual(
            set([n['name'] for n in s.p1.bothE().end]), set([u'Anne', 'Bob', 'Eugene'])
            )
        
    def test_groupby(s):
        people = s.gt.fetch(CHAIN='[p:Person,likecount] -(e:Likes)->', likecount='COUNT(e.uid)', GROUP='p.uid')
        s.assertCountEqual( people.get('_likecount'), [1,1,2,2] )

        
    def test_start(s):
        s.assertEqual(
            set([n['name'] for n in s.p5.inE().start]), set(['Anne', 'Dirk','Charlotte'])
            )
        
    def test_one(s):
        s.assertEqual(
            s.p2.outE(WHERE='e.kind = "Follows"').end.one['name'], 'Fred'
            )

    def test_inoutLen(s):
        bob = s.gt.fetch(WHERE='n.data.name = "Bob"').one
        
        s.assertCountEqual(
            [len(bob.inE()), len(bob.outE()), 
             len(bob.inN()), len(bob.outN()), 
             len(bob.bothE()),len(bob.bothN())],
            [2, 3, 1, 3, 5, 3])
        
    def test_inoutCount(s):
        bob = s.gt.fetch(WHERE='n.data.name = "Bob"').one
        
        s.assertCountEqual(
            [bob.inE(COUNT=True), bob.outE(COUNT=True), 
             bob.inN(COUNT=True), bob.outN(COUNT=True), 
             bob.bothE(COUNT=True),bob.bothN(COUNT=True)],
            [2, 3, 1, 3, 5, 3])
        
    def test_nodefts(s):
        s.assertEqual(
            s.gt.fetch(CHAIN='(n)', n_fts="An*").one['name'], 'Anne'
            )
        
    def test_neighbours(s):
        s.assertTrue(
            s.gt.fetch(CHAIN='[n1] -(e:Likes)>')==s.gt.fetch(CHAIN='-(e:Likes)>').start
            )
        
    def test_save(s):
        a=s.gt.fetch(CHAIN='(n)')[0]
        a['extra']='hello'
        a.save()     
        
        b = s.gt.getuid(a['uid'])
        s.assertEqual( b['extra'], 'hello')
        
    def test_delete(s):
        # deleting and saving will break all the connections but retain the data
        s.p1.delete(disconnect=True)
        s.p1.save()  
        
        # find all the unconnected nodes (should only be p1)
        a=( s.gt.fetch('(n)')-s.gt.fetch('-(e)> [n]')-s.gt.fetch('[n] -(e)>') ).one
        s.assertEqual(a['uid'] , s.p1['uid'] )
        
    def test_limitcount(s):
        vs=s.gt.fetch('(n)', LIMIT=3)
        s.assertTrue( len(vs)==3 )
        
    def test_filter(s):
        s.assertEqual(
            s.gt.fetch(CHAIN='(n)').filter(lambda n: n['name']=='Anne').one['name'], 'Anne'
            )
        
    def test_sort(s):
        vs=s.gt.fetch(CHAIN='(n)')
        vs.sort(key=lambda x:x['name'])
        
        s.assertEqual(
            [n['name'] for n in vs], ['Anne', 'Bob', 'Charlotte', 'Dirk', 'Eugene', 'Fred']
            )

    def test_extra(s):
        s.assertEqual(set(s.gt.fetch(CHAIN='[n,nn]',nn='n.data.name').get('_nn')),
                      set(['Dirk', 'Bob', 'Eugene', 'Charlotte', 'Anne', 'Fred']))  

    def test_stats(s):
        stats = s.gt.stats
        s.assertTrue ( stats['Edge kinds'] == {'Follows': 4, 'Likes': 6} and 
                       stats['Node kinds'] == {'Person': 6} and
                       stats['Total edges'] == 10 and 
                       stats['Total nodes'] == 6
                       )
        
class CompleteGraphTests(unittest.TestCase):
    
    def setUp(s):
        # set up a complete graph
        
        s.g = Graph() 
        
        s.nodes = NSet([s.g.Node('Person').save() for n in range(10)])
        
        for n1 in s.nodes:
            for n2 in s.nodes:
                s.g.Edge(n1,'E', n2).save()        
                
    def test_stats(s):
        stats = s.g.stats
        s.assertTrue ( stats['Edge kinds'] == {'E': 100} and 
                       stats['Node kinds'] == {'Person': 10} and
                       stats['Total edges'] == 100 and 
                       stats['Total nodes'] == 10
                       )    
        
    def test_outN(s):
        s.assertEqual(s.nodes.outN(),s.nodes)
        
    def test_inN(s):
        s.assertEqual(s.nodes.inN(),s.nodes)
        
    def test_bothN(s):
        s.assertEqual(s.nodes.bothN(),s.nodes)
        
    def test_loops1(s):
        loops = s.g.fetch('(n1) -[e]> (n2)','n1.uid == n2.uid')
        s.assertEqual(loops.end,s.nodes)
        
    def test_loops2(s):
        loops = s.g.fetch('-[e]>','e.startuid == e.enduid')
        s.assertEqual(loops.end,s.nodes)
        
    def test_loops3(s):
        loops = s.g.fetch('-[e]>','e.startuid == e.enduid')
        s.assertEqual(loops.end,loops.start)
        
    def test_chain(s):
        s.assertEqual(
            s.g.fetch('(n1) -(e)> (n2) -(e2)> (n3) -(e3)> (n4)','n1.uid == :n1uid', n1uid=s.nodes[0]['uid']),
            s.nodes)
    
class ExampleGraphTests(unittest.TestCase):
    
    def setUp(s):

        g = Graph()
        
        # create some nodes and immediately store them in the graph
        anne = g.Node('Person', name="Anne", age=22).save()
        bob = g.Node('Person', name="Bob", nickname='Bobby', age=19).save()
        charlie = g.Node('Person', name="Charlie", age=31).save()
        
        # alternative ways of creating nodes
        coffee = Node({'kind':'Drink', 'sort':'Coffee'}, graph=g).save()
        tea = Node({'kind':'Drink', 'sort':'Coffee'})
        tea.setGraph(g)
        tea.save()
        
        # connect some nodes with edges
        g.Edge(anne, 'Likes', bob).save()
        g.Edge(charlie['uid'], 'Likes', bob['uid']).save()
        
        # alternative edge creation and saving
        e1 = Edge({'startuid':anne['uid'], 'kind':'Drinks', 'enduid':coffee['uid'], 'strength':'strong'}, graph=g).save()
        data = {'startuid':charlie['uid'], 'kind':'Drinks', 'enduid':tea['uid']}
        e2 = Edge(data)
        e2.setGraph(g)
        e2.save()
        g.Edge(enduid=tea, startuid=bob, kind='Drinks', strength='strong', preference=1).save()
        g.Edge(bob,'Drinks',coffee, strength='weak', preference=0).save()     
        
        s.g=g

    def test_stats_aftercreation(s):
        stats = s.g.stats
        s.assertEqual( stats['Changes'], 11)
        
    def test_connected_delete(s):
        bob = s.g.fetch('(n)','n.data.name = "Bob"')
        s.assertRaises(GraphyDBException, bob.delete)
        
    def test_undo_delete(s):
        bob = s.g.fetch('(n)','n.data.name = "Bob"').one
        bob.delete(disconnect=True)
        before = bob.exists # should be false
        s.g.undo()
        after = bob.exists # should be true again
        s.assertTrue( after and not before )

    def test_undo_change(s):
        bob = s.g.fetch('(n)','n.data.name = "Bob"').one
        bob['nickname'] = 'Bobs The Impaler'
        bob.save()
        before = bob.original()['nickname']
        s.g.undo()
        after = bob.original()['nickname']
        s.assertTrue( before=='Bobs The Impaler' and after=='Bobby' )
        
    def test_add_node(s):
        new = s.g.Node('Test').save()
        before = new.exists
        s.g.undo()
        after = new.exists
        s.assertTrue( before and not after )
        
    def test_add_edge(s):
        bob = s.g.fetch('(n)','n.data.name = "Bob"').one
        anne = s.g.fetch('(n)','n.data.name = "Anne"').one
        new = s.g.Edge(bob,'Test',anne).save()
        before = new.exists
        s.g.undo()
        after = new.exists
        s.assertTrue( before and not after )