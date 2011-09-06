#-*- coding: utf-8 -*-
"""
This is python implementation of Ramer-Douglas-Peucker algorithm
Author: Dmitri Lebedev
Url: http://ryba4.com/python/ramer-douglas-peucker

Code is modified to use with metric generator.
"""
#from sys import exit
import time


def ramerdouglas(line, dist, epsilons, thinned_data_len):
    """Does Ramer-Douglas-Peucker simplification of  a line with `dist` threshold.
    `line` must be a list of Vec objects,
    """
    if len(line) < 3:
        return
    
    begin, end = line[0], line[-1]
    
    distSq_dict = [{'dist':begin.distSq(curr) -
        ((end - begin) * (curr - begin)) ** 2 /
        begin.distSq(end),'id':curr.id} for curr in line[1:-1]]
#    """
#    distSq_dict = {}
#    for curr in line[1:-1]:
#        distSq_dict[curr.id] = begin.distSq(curr) - ((end - begin) * (curr - begin)) ** 2 / begin.distSq(end)
#    """
    distSq = []
    
#    """
#    for id in distSq_dict:
#        distSq.append(distSq_dict[id])
#        if not epsilons.has_key(id) or epsilons[id] > distSq_dict[id]:
#            epsilons[id] = distSq_dict[id]
#    """
    
    for cur_dist in distSq_dict:
        distSq.append(cur_dist['dist'])
        #cur_dist['dist'] = cur_dist['dist']/(Decimal('100')*(Decimal(str(end.id)) - Decimal(str(begin.id))))*Decimal(str(end.id))
        #cur_dist['dist'] = cur_dist['dist']*(((Decimal(str(end.id)) - Decimal(str(begin.id))))*Decimal(str(end.id)))
        cur_dist['dist'] = cur_dist['dist']/thinned_data_len*((((end.id - begin.id)))**2)
        epsilons.append(cur_dist)
            
    maxdist = max(distSq)
    if maxdist < dist ** 2:
        return
    
    pos = distSq.index(maxdist)
    ramerdouglas(line[:pos + 2], dist, epsilons, thinned_data_len)
    ramerdouglas(line[pos + 1:], dist, epsilons, thinned_data_len)




class Line:
    """Polyline. Contains a list of points and outputs
    a simplified version of itself."""
    def __init__(self, points):
        self.points = points
    
    def simplify(self, dist, epsilons, thinned_data_len):
        self.points[:-1]
        
        #points = ramerdouglas(self.points[:-1], dist, epsilons, thinned_data_len) + self.points[-1:]
        
        ramerdouglas(self.points[:-1], dist, epsilons, thinned_data_len)
      
        
        #return self.__class__(points)

class Vec:
    """Generic vector class for n-dimensional vectors
    for any natural n."""
    def __eq__(self, obj):
        return self.coords == obj.coords
    
    def __add__(self, obj):
        """Add a vector."""
        
        coords = map(sum, zip(self.coords, obj.coords))
        return self.__class__(coords[0], coords[1], self.id)
    
    def __neg__(self):
        """Reverse the vector."""
        return self.__class__(-self.coords[0], -self.coords[1], self.id)

    
    def __sub__(self, obj):
        """Substract object from self."""
        if not isinstance(obj, self.__class__):
            raise TypeError
        
        return self + (- obj)

    def __mul__(self, obj):
        """If obj is scalar, scales the vector.
        If obj is vector returns the scalar product."""
        if isinstance(obj, self.__class__):
            return sum([a * b for (a, b) in zip(self.coords, obj.coords)])
        
        return self.__class__(*[i * obj for i in self.coords])

    def dist(self, obj = None):
        """Distance to another object. Leave obj empty to get
        the length of vector from point 0."""
        return self.distSq(obj) ** 0.5

    def distSq(self, obj = None):
        """ Square of distance. Use this method to save
        calculations if you don't need to calculte an extra square root."""
        if obj is None:
            obj = self.__class__(*[0]*len(self.coords))
        
        
        # simple memoization to save extra calculations
        if obj.coords not in self.distSqMem:
            self.distSqMem[obj.coords] = sum([(s - o) ** 2 for (s, o) in
                zip(self.coords, obj.coords)])
        return self.distSqMem[obj.coords]

class Vec2D(Vec):
    """2D vector"""
    def __init__(self, x, y, id):
        self.coords = x, y
        self.id = id
        self.distSqMem = {}
