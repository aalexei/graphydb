import unittest
import random

import os,sys
parentdir = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0,parentdir)

from graphydb import IndexedSet

class O:
    def __init__(self, v):
        self.v = v
        
    def __uid__(self):
        return str(self.__hash__())
    
    def __hash__(self):
        return hash(self.v)
    
    def __eq__(self, other):
        return self.__hash__() == other.__hash__()
        

class TestIndexedSet_setops(unittest.TestCase):
    
    def setUp(s):
        
        
        A = [O(random.randint(1,40)) for a in range(30)]
        B = [O(random.randint(1,40)) for a in range(30)]
        C = [O(random.randint(1,40)) for a in range(30)]
        s.A1 = set(A)
        s.B1 = set(B)
        s.C1 = set(C)
        s.A2 = IndexedSet(A)
        s.B2 = IndexedSet(B)
        s.C2 = IndexedSet(C)   
        
        
    def test_symdiff(s):
        s.assertEqual(s.A1 ^ s.B1, set(s.A2 ^ s.B2))
        
    def test_diff(s):
        s.assertEqual(s.A1 - s.B1, set(s.A2 - s.B2))        
        
    def test_union(s):
        s.assertEqual(s.A1 | s.B1, set(s.A2 | s.B2))  

    def test_intersection(s):
        s.assertEqual(s.A1 & s.B1, set(s.A2 & s.B2))  

    def test_multop1(s):
        s.assertEqual(s.A1 & s.B1 ^ s.C1, set(s.A2 & s.B2 ^ s.C2))        

    def test_multop2(s):
        s.assertEqual(s.A1 & s.B1 | s.C1, set(s.A2 & s.B2 | s.C2)) 
        
    def test_multop3(s):
        s.assertEqual(s.A1 - s.B1 | s.C1, set(s.A2 - s.B2 | s.C2)) 
        
    def test_ge(s):
        s.assertTrue(s.A2 >= s.A2[:2])
        
    def test_le(s):
        s.assertTrue(s.A2[:2] <= s.A2)
        
    def test_copyeq(s):
        s.assertTrue(s.A2[:] == s.A2)
        
    def test_ne(s):
        s.assertTrue(s.A2[:2] != s.B2)  
        
    def test_selfge(s):
        s.assertTrue(s.A2 >= s.A2)
        
    def test_symdiffmethod(s):
        s.assertEqual(s.A1.symmetric_difference(s.B1), set(s.A2.symmetric_difference(s.B2)))
        
    def test_diffmethod(s):
        s.assertEqual(s.A1.difference(s.B1), set(s.A2.difference(s.B2)))
        
    def test_unionmethod(s):
        s.assertEqual(s.A1.union(s.B1), set(s.A2.union(s.B2)))
        
    def test_intersectionmethod(s):
        s.assertEqual(s.A1.intersection(s.B1), set(s.A2.intersection(s.B2)))

    def test_multdiffmethod(s):
        s.assertEqual(s.A1.difference(s.B1,s.C1), set(s.A2.difference(s.B2,s.C2)))
        
    def test_multunionmethod(s):
        s.assertEqual(s.A1.union(s.B1,s.C1), set(s.A2.union(s.B2,s.C2)))
        
    def test_multintersectionmethod(s):
        s.assertEqual(s.A1.intersection(s.B1,s.C1), set(s.A2.intersection(s.B2,s.C2)))

class TestIndexedSet_listops(unittest.TestCase):
    
    def setUp(s):
        
        s.a = IndexedSet([O(random.randint(1,40)) for a in range(30)])
        s.b = IndexedSet([O(random.randint(1,40)) for a in range(30)])
    
    def test_len(s):
        s.assertEqual( len(s.a), len(s.a._list) )
        
    def test_listandindex(s):
        c = s.a-s.b
        s.assertEqual(set(c._index.values()) ,set(c._list))
 
    def test_sort(s):
        s.a.sort(key=lambda n:n.v)
        s.assertLess(s.a[0].v, s.a[-1].v)       
        
    def test_sortreverse(s):
        s.a.sort(key=lambda n:n.v, reverse=True)
        s.assertGreater(s.a[0].v, s.a[-1].v) 
        
    def test_sort2(s):
        s.a.sort(key=lambda n:n.v)
        a0 = s.a[0]
        s.a.sort(key=lambda n:n.v, reverse=True)
        s.assertIs(a0, s.a[-1])   
        
    def test_update1(s):
        da = dict(s.a._index)
        da.update(s.b._index)
        
        s.a.update(s.b)
        s.assertEqual( s.a._index, da)
        
    def test_clear(s):
        s.a.clear()
        s.assertTrue( s.a._list ==[] and s.a._index == {})
        
    def test_popandslice(s):
        a2 = IndexedSet(s.a)
        a2.pop()
        s.assertEqual( s.a[:-1], a2 )
        
    def test_delslice(s):
        a2 = IndexedSet(s.a)
        del a2[:10]
        s.assertEqual( s.a[10:], a2 )
        
    def test_del(s):
        a2 = IndexedSet(s.a)
        del a2[0]
        s.assertEqual( s.a[1:], a2 )    
        
    def test_remove(s):
        a2 = IndexedSet(s.a)
        a2.remove(a2[-1])
        s.assertEqual( s.a[:-1], a2 )  
        
    def test_removeexception(s):
        s.assertRaises(KeyError, s.a.remove, O(-100))
        
    def test_discard(s):
        a2 = IndexedSet(s.a)
        a2.discard(a2[-1])
        s.assertEqual( s.a[:-1], a2 )      

    def test_copy1(s):
        b = s.a.copy()
        s.assertEquals( s.a , b )
        
    def test_copy2(s):
        b = s.a.copy()
        b.pop()
        s.assertNotEquals( s.a , b )