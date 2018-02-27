#!/usr/bin/env python3
# vim:ts=4:sts=4:sw=4:expandtab

import json
import logging
import random
import uuid
import queue

###
#
# Interfaces
#
###
import time


class Packet:
    """Abstract packet class"""
    def __init__(self, src, dst):
        self._id  = uuid.uuid4()
        self._src = src
        self._dst = dst
    @property
    def id(self):
        """Returns globally unique id of the packet"""
        return self._id
    @property
    def src(self):
        """Returns address of the source router"""
        return self._src
    @property
    def dst(self):
        """Returns address of the destination router"""
        return self._dst

class MetaPacket(Packet):
    """Packet for routing algorithm communication"""
    def __init__(self, src, dst, payload):
        super().__init__(src, dst)
        self._payload = json.dumps(payload)
    @property
    def payload(self):
        return json.loads(self._payload)

class Link:
    """Abstract inter-router link class"""
    def __init__(self, dst):
        self._dst = dst
    @property
    def dst(self):
        """Returns address of the destination router"""
        return self._dst

class Router:
    """Abstract router class"""
    @property
    def id(self):
        """Returns address of the router"""
        pass
    @property
    def links(self):
        """Returns a list of links available at the router"""
        pass
    @property
    def stored_packets(self):
        """Returns a list of packets stored in the memory of the router"""
        pass
    def drop_packet(self, packet):
        """Drops a packet"""
        pass
    def store_packet(self, packet):
        """Stores a packet in the memory of the router"""
        pass
    def forward_packet(self, link, packet):
        """Forwards a packet over a link"""
        pass

class RoutingAlgorithm:
    """Abstract routing algorithm class"""
    def __init__(self, router):
        if not isinstance(router, Router):
            raise ValueError
        self.router = router
    def __call__(self, packets):
        if not isinstance(packets, list):
            raise ValueError
        for src, packet in packets:
            if not isinstance(packet, Packet):
                raise ValueError
            if src is not None and not isinstance(src, Link):
                raise ValueError
        self.route(packets)
    def add_link(self, link):
        """Called when new link is added to router"""
        pass
    def del_link(self, link):
        """Called when a link is removed from router"""
        pass
    def route(self, packets):
        """Called in every round of routing algorithm"""
        pass

###
#
# Simulation engine
#
###
class Simulator:
    """Simulator sandbox for routing algorithm experiments"""
    class SimPacket(Packet):
        def __init__(self, src, dst, start_time):
            super().__init__(src, dst)
            self.start_time = start_time
            self.stop_time = None

    class SimLink(Link):
        def __init__(self, dst):
            super().__init__(dst)
            self.packet = None

        def forward_packet(self, packet):
            if self.packet is not None:
                raise RuntimeError
            if not isinstance(packet, Packet):
                raise ValueError
            self.packet = packet

    class SimRouter(Router):
        def __init__(self, algorithm_class, id=None):
            if not issubclass(algorithm_class, RoutingAlgorithm):
                raise ValueError
            super().__init__()
            self._id = id or uuid.uuid4()
            self._links = dict()
            self.store = dict()
            self.packets = dict()
            self.algorithm = algorithm_class(self)

        @property
        def id(self):
            return self._id
        @property
        def links(self):
            return list(self._links.values())
        @property
        def stored_packets(self):
            return list(self.store.values())

        def drop_packet(self, packet):
            if not isinstance(packet, Packet):
                raise ValueError
            if packet.id in self.store:
                del self.store[packet.id]
            if packet.id in self.packets:
                del self.packets[packet.id]
            logging.info("Droped packet [{}] {} -> {}".format(packet.id, packet.src, packet.dst))

        def store_packet(self, packet):
            if not isinstance(packet, Packet):
                raise ValueError
            self.store[packet.id] = packet
            if packet.id in self.packets:
                del self.packets[packet.id]

        def forward_packet(self, link, packet):
            if not isinstance(link, Simulator.SimLink):
                raise ValueError
            if not isinstance(packet, Packet):
                raise ValueError
            if link not in self.links:
                raise ValueError
            if isinstance(packet, Simulator.SimPacket):
                if packet.id not in self.store and packet.id not in self.packets:
                    raise ValueError
            link.forward_packet(packet)
            if packet.id in self.store:
                del self.store[packet.id]
            if packet.id in self.packets:
                del self.packets[packet.id]

    def __init__(self):
        self.routers = dict()
        self.links = set()
        self.time = 0
        self.routable_packets = 0
        self.routed_packets = list()

    @property
    def stats(self):
        response = dict()
        response['packets'] = self.routable_packets
        if self.routable_packets > 0:
            response['delivery_rate'] = len(self.routed_packets) / self.routable_packets
        response['routed'] = len(self.routed_packets)
        if len(self.routed_packets) > 0:
            response['avg_time'] = sum( [p.stop_time - p.start_time for p in self.routed_packets] ) / len(self.routed_packets)
        return response

    def add_router(self, algorithm_class, id=None):
        if id in self.routers:
            raise ValueError
        r = Simulator.SimRouter(algorithm_class, id)
        self.routers[r.id] = r
        return r

    def add_link(self, r1, r2):
        if isinstance(r1, Router):
            r1 = r1.id
        if isinstance(r2, Router):
            r2 = r2.id
        if r1 not in self.routers or r2 not in self.routers:
            raise ValueError
        r1, r2 = (min(r1,r2), max(r1,r2))
        if r1 != r2 and (r1,r2) not in self.links:
            self.links.add( (r1,r2) )
            self.routers[r1]._links[r2] = Simulator.SimLink(r2)
            self.routers[r1].algorithm.add_link(self.routers[r1]._links[r2])
            self.routers[r2]._links[r1] = Simulator.SimLink(r1)
            self.routers[r2].algorithm.add_link(self.routers[r2]._links[r1])

    def del_link(self, r1, r2):
        if isinstance(r1, Router):
            r1 = r1.id
        if isinstance(r2, Router):
            r2 = r2.id
        if r1 not in self.routers or r2 not in self.routers:
            raise ValueError
        r1, r2 = (min(r1,r2), max(r1,r2))
        if (r1,r2) in self.links:
            self.links.remove( (r1,r2) )
            self.routers[r1].algorithm.del_link(self.routers[r1]._links[r2])
            del self.routers[r1]._links[r2]
            self.routers[r2].algorithm.del_link(self.routers[r2]._links[r1])
            del self.routers[r2]._links[r1]

    def add_packet(self, r1, r2):
        if isinstance(r1, Router):
            r1 = r1.id
        if isinstance(r2, Router):
            r2 = r2.id
        if r1 in self.routers:
            if r2 in self.routers:
                self.routable_packets += 1
            router = self.routers[r1]
            packet = Simulator.SimPacket(r1, r2, self.time)
            router.packets[packet.id] = (None, packet)
            return packet

    def route(self):
        self.time += 1
        for id, router in self.routers.items():
            router.algorithm(list(router.packets.values()))
            for src, packet in router.packets.values():
                if packet.dst != router.id:
                    logging.warning("Silently droped packet [{}] {} -> {} at {}".format(packet.id, packet.src, packet.dst, router.id))
            router.packets = dict()
        for id, router in self.routers.items():
            for link in router.links:
                if link.packet is not None:
                    packet = link.packet
                    link.packet = None
                    if link.dst in self.routers:
                        if isinstance(packet, Simulator.SimPacket) and packet.dst == link.dst:
                            packet.stop_time = self.time
                            self.routed_packets.append(packet)
                            logging.info("Routed packet [{}] {} -> {} in {} steps".format(packet.id, packet.src, packet.dst, packet.stop_time - packet.start_time))
                        else:
                            logging.debug("Forwarded packet [{}] {} -> {} to {}".format(packet.id, packet.src, packet.dst, link.dst))
                            self.routers[link.dst].packets[packet.id] = (self.routers[link.dst]._links[router.id], packet)

###
#
# Routing algorithms
#
###
class RandomRouter(RoutingAlgorithm):
    """Routing algorithm that forwards packets in random directions"""
    def route(self, packets):
        for src, packet in packets:
            self.router.store_packet(packet)
        packets = self.router.stored_packets
        random.shuffle(packets)
        links = self.router.links
        random.shuffle(links)
        for link in links:
            if len(packets) > 0:
                self.router.forward_packet(link, packets[-1])
                packets = packets[0:-1]

class ShortPathRouter(RoutingAlgorithm):
    """Distance vector type routing algorithm"""

    def __init__(self, router):
        super().__init__(router)
        self.tick = 0
        # key(destiny):value([neighbour, distance])
        self.distance_vec = dict()
        self.deleted = False
        self.last_delete = -1

    @property
    def distance_vector(self):
        return self.distance_vec

    def route(self, packets):
        for src, packet in packets:
            if isinstance(packet, MetaPacket):
                logging.debug(
                    'Router {} received vector {} from {}'.format(self.router.id, packet.payload, src.dst))
                if isinstance(packet.payload, int):
                    if self.last_delete >= packet.payload:
                        continue
                    self.deleted = True
                    self.last_delete = packet.payload
                    self.distance_vec = dict()
                    for link in self.router.links:
                        self.distance_vec[link._dst] = (1, link._dst)
                    continue
                for key, value in packet.payload.items():
                    if key not in self.distance_vec or (value[0] + 1) < self.distance_vec[key][0]:
                        self.distance_vec[key] = (value[0] + 1, packet.src)
            else:
                self.router.store_packet(packet)

        if self.tick % 5 == 0:
            if self.deleted:
                self.deleted = False
                self.distance_vec = dict()
                for link in self.router.links:
                    self.distance_vec[link._dst] = (1, link._dst)
                for link in self.router.links:
                    self.router.forward_packet(link, MetaPacket(self.router.id, link.dst, self.last_delete))
            else:
                logging.debug(
                    'Router {} sending vector {} to neighbors'.format(self.router.id, self.distance_vector))
                for link in self.router.links:
                    self.router.forward_packet(link, MetaPacket(self.router.id, link.dst, self.distance_vector))
        else:
            for packet in self.router.stored_packets:
                for link in self.router.links:
                    if packet.dst in self.distance_vec and link.dst == self.distance_vec[packet.dst][1] and link.packet is None:
                        self.router.forward_packet(link, packet)
                        break

        self.tick += 1

    def add_link(self, link):
        self.distance_vec[link._dst] = (1, link._dst)

    def del_link(self, link):
        self.deleted = True
        self.last_delete = self.tick
        self.distance_vec = dict()
        for link in self.router.links:
            self.distance_vec[link._dst] = (1, link._dst)

class GraphRouting(RoutingAlgorithm):
    """Graph routing type routing algorithm"""

    def __init__(self, router):
        super().__init__(router)
        self.tick = 0
        #key(vertex):value(key(vertex):value(time, is_existing))
        self.graph = dict()
        self.zmiana = False

    def route(self, packets):
        for src, packet in packets:
            if isinstance(packet, MetaPacket):
                logging.debug(
                    'Router {} received vector {} from {}'.format(self.router.id, packet.payload, src.dst))
                for v,x in packet.payload.items():
                    for u,tup in x.items():
                        if v not in self.graph:
                            self.graph[v] = dict()
                        elif u in self.graph[v] and self.graph[v][u][0] >= tup[0]:
                            continue
                        self.graph[v][u] = tup
                        self.zmiana = True
            else:
                self.router.store_packet(packet)
        if self.zmiana or self.tick % 20 == 0:
            for link in self.router.links:
                self.router.forward_packet(link, MetaPacket(self.router.id, link.dst, self.graph))
            self.zmiana = False
        else:
            for packet in self.router.stored_packets:
                x = self.find_neighbour(packet.dst)
                if x is not None:
                    self.router.forward_packet(x, packet)
        self.tick += 1

    def find_neighbour(self, dst):
        p = dict()
        p[self.router.id] = self.router.id
        q = queue.Queue()
        for link in self.router.links:
            if link.packet is None:
                p[link.dst] = self.router.id
                q.put(link.dst)
        while not q.empty():
            v = q.get()
            if v == dst:
                break
            if v not in self.graph:
                continue
            for u,x in self.graph[v].items():
                if u in p or not x[1]:
                    continue
                p[u] = v
                q.put(u)
        if dst not in p:
            return None
        while p[dst] != self.router.id:
            dst = p[dst]
        for link in self.router.links:
            if link.dst == dst:
                return link
        return None

    def add_link(self, link):
        v = self.router.id
        u = link.dst
        if v not in self.graph:
            self.graph[v] = dict()
        self.graph[v][u] = (self.tick, True)
        self.zmiana = True

    def del_link(self, link):
        v = self.router.id
        u = link.dst
        if v not in self.graph:
            self.graph[v] = dict()
        self.graph[v][u] = (self.tick, False)
        self.zmiana = True

###
#
# Simulation scenario
#
###
def test1(algorithm):
    logging.basicConfig(level=logging.DEBUG)
    sim = Simulator()
    algo = algorithm
    r1 = sim.add_router(algo, 'a')
    r2 = sim.add_router(algo, 'b')
    r3 = sim.add_router(algo, 'c')
    r4 = sim.add_router(algo, 'd')
    sim.add_link(r1, r2)
    sim.add_link(r2, r3)
    sim.add_link(r3, r4)
    sim.add_packet(r1, r4)
    for i in range(50):
        if i % 2 == 0:
            sim.add_packet(r1, r4)
        sim.route()
    for i in range(5):
        sim.route()
    print(sim.stats)

def test2(algorithm):
    logging.basicConfig(level=logging.DEBUG)
    sim = Simulator()
    algo = algorithm
    r1 = sim.add_router(algo, 'a')
    r2 = sim.add_router(algo, 'b')
    r3 = sim.add_router(algo, 'c')
    r4 = sim.add_router(algo, 'd')
    r5 = sim.add_router(algo, 'e')
    r6 = sim.add_router(algo, 'f')
    r7 = sim.add_router(algo, 'g')
    r8 = sim.add_router(algo, 'h')
    sim.add_link(r1, r2)
    sim.add_link(r2, r3)
    sim.add_link(r3, r4)
    sim.add_link(r4, r5)
    sim.add_link(r5, r6)
    sim.add_link(r6, r7)
    sim.add_link(r7, r8)
    sim.add_link(r8, r1)
    sim.add_link(r2, r6)
    for i in range(70):
        if i % 3 == 0:
            sim.add_packet(r1, r6)
        elif i % 3 == 1:
            sim.add_packet(r2, r4)
        if i % 25 == 0:
            sim.del_link(r2, r3)
            sim.del_link(r5, r6)
        if i % 30 == 0:
            sim.add_link(r2, r3)
            sim.add_link(r5, r6)
        sim.route()

    for i in range(50):
        sim.route()
    print(sim.stats)


def test3(algorithm):
    logging.basicConfig(level=logging.DEBUG)
    sim = Simulator()
    algo = algorithm
    r1 = sim.add_router(algo, 'a')
    r2 = sim.add_router(algo, 'b')
    r3 = sim.add_router(algo, 'c')
    r4 = sim.add_router(algo, 'd')
    r5 = sim.add_router(algo, 'e')
    r6 = sim.add_router(algo, 'f')
    r7 = sim.add_router(algo, 'g')
    r8 = sim.add_router(algo, 'h')
    sim.add_link(r1, r2)
    sim.add_link(r2, r3)
    sim.add_link(r3, r4)
    sim.add_link(r4, r1)
    sim.add_link(r5, r6)
    sim.add_link(r6, r7)
    sim.add_link(r7, r8)
    sim.add_link(r8, r5)
    sim.add_link(r3, r8)
    for i in range(120):
        if i % 2 == 0:
            sim.add_packet(r2, r7)
        if i % 5 == 0:
            sim.add_packet(r1, r3)
        if i == 30:
            sim.del_link(r1, r6)
            sim.del_link(r1, r2)
        if i == 70:
            sim.add_link(r1, r2)
        sim.route()

    sim.add_link(r1, r6)
    for i in range(54):
        sim.route()
    print(sim.stats)

def test4(algorithm):
    logging.basicConfig(level=logging.DEBUG)
    sim = Simulator()
    algo = algorithm
    r1 = sim.add_router(algo, 'a')
    r2 = sim.add_router(algo, 'b')
    r3 = sim.add_router(algo, 'c')
    r4 = sim.add_router(algo, 'd')
    r5 = sim.add_router(algo, 'e')
    r6 = sim.add_router(algo, 'f')
    sim.add_link(r1, r3)
    sim.add_link(r2, r3)
    sim.add_link(r3, r4)
    sim.add_link(r4, r5)
    sim.add_link(r4, r6)

    for i in range(200):
        if i % 3 == 0:
            sim.add_packet(r1, r5)
        if i % 40 == 0:
            sim.del_link(r3, r4)
        if i % 40 == 5:
            sim.add_link(r3, r4)
        sim.route()

        for i in range(150):
            sim.route()
        print(sim.stats)
###
#
# test functions with parametr - algoritm name (ShortPathRouter or GraphAlgoritm)
#
# Options:
# test1(ShortPathRouter)
# test2(ShortPathRouter)
# test3(ShortPathRouter)
# test4(ShortPathRouter)
# test1(GraphRouting)
# test2(GraphRouting)
# test3(GraphRouting)
# test4(GraphRouting)
#
###

test4(GraphRouting)
