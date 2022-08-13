
import * as THREE from "https://unpkg.com/three@0.143.0/build/three.module.js"

export class Node {
    constructor(key, mv, nbrs, faulty) {
        this.key = key
        this.mv = mv
        this.nbrs = nbrs
        this.spheres = []
        this.faulty = faulty
    }
}

export class Nodes {
    constructor() {
        this.nodes = []
    }
    newNode(key, mv, nbrs, faulty) {
        let n = new Node(key, mv, nbrs, faulty)
        this.nodes.push(n)
        return n
    }
    nth(key) {
        return this.nodes[key]
    }
    allNodes() {
        return this.nodes
    }
    link(from, fromlevel, to, tolevel) {
        let material
        if (this.nodes[from].faulty) {
            material = new THREE.LineBasicMaterial( { color: 0xbb1597 } );
        } else {
            material = new THREE.LineBasicMaterial( { color: 0x1597bb } );
        }
        let points = []
        //if (this.nodes[from].pos.length > level + 1) {
        points.push(this.nodes[from].spheres[fromlevel].position)
        points.push(this.nodes[to].spheres[tolevel].position)
        const geo = new THREE.BufferGeometry().setFromPoints(points);
        return new THREE.Line(geo, material);
        //}
        //return null
    }
}