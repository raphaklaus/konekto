const uuid = require('uuid/v4')
const SqlWhereParser = require('sql-where-parser')
const yielded = Symbol('yielded')
const isRelated = Symbol('isRelated')
const labelRegex = /^[a-z$][a-z_0-9]+$/
const nodeRegex = /(?<label>\w+)\[(?<id>[\d.]+)\](?<node>\{.*\})/g
const relationshipRegex = /(?<label>\w+)\[(?<id>[\d.]+)\]\[(?<from>[\d.]+),(?<to>[\d.]+)\](?<metadata>\{.*?\})/g
const config = SqlWhereParser.defaultConfig
config.operators[5].STARTSWITH = 2
config.operators[5].ENDSWITH = 2
config.operators[5].CONTAINS = 2
const sqlWhereParser = new SqlWhereParser(config)
const { EventEmitter } = require('events')
function isPrimitive (value) {
  return (
    typeof value !== 'object' ||
    (Array.isArray(value) && (typeof value[0] !== 'object' || value[0] instanceof Date)) ||
    value instanceof Date
  )
}
function * iterateJson (child, key, parent, metadata) {
  if (child && typeof child === 'object' && !Array.isArray(child)) {
    if (!child.konekto_id) {
      child.konekto_id = uuid()
    }
    const reference = child.konekto_id
    const result = {
      konekto_id: child.konekto_id,
      _id: child._id,
      _label: child._label
    }
    if (child._label) {
      validateLabel(child._label)
    }
    const nextIteration = []
    for (const [key, value] of Object.entries(child)) {
      if (key.startsWith('_')) {
        continue
      }
      validateLabel(key)
      if (isPrimitive(value)) {
        result[key] = value
      } else if (Array.isArray(value) && typeof value[0] === 'object') {
        for (const item of value) {
          nextIteration.push({
            child: item,
            key,
            parent: reference,
            metadata: { isArray: true }
          })
        }
      } else {
        nextIteration.push({
          child: value,
          key,
          parent: reference
        })
      }
    }
    yield {
      object: child,
      child: result,
      key,
      parent,
      metadata
    }

    if (!child[yielded]) {
      child[yielded] = true
      for (const iteration of nextIteration) {
        yield * iterateJson(iteration.child, iteration.key, iteration.parent, iteration.metadata)
      }
    }

    delete child.konekto_id
    delete child[yielded]
  }
}

function getNodesAndRelationships (json) {
  const nodes = {}
  const objects = {}
  const relationships = []
  for (const item of iterateJson(json)) {
    nodes[item.child.konekto_id] = item.child
    objects[item.child.konekto_id] = item.object
    relationships.push({
      from: item.parent,
      name: item.key,
      to: item.child.konekto_id,
      metadata: item.metadata
    })
  }
  const root = nodes[relationships.shift().to]
  return {
    objects,
    rootObject: json,
    root,
    nodes,
    relationships
  }
}

function getIndexesPerNode (nodes) {
  const indexesPerNode = {}
  const nodeIds = Object.keys(nodes)
  for (let index = 0; index < nodeIds.length; index++) {
    const nodeId = nodeIds[index]
    indexesPerNode[nodeId] = index + 1
  }
  return indexesPerNode
}

function getWhereCypher (params, json, variableName) {
  const whereQuery = ['WHERE']
  if (json._label) {
    let whereLabels
    if (typeof json._label === 'string') {
      whereLabels = [json._label]
    } else {
      whereLabels = json._label
    }
    whereLabels = whereLabels.map(l => `label(${variableName}) = '${l}'`).join(' OR ')
    whereQuery.push(whereLabels)
  }
  if (json.where) {
    if (json._label) {
      whereQuery.push('AND')
    }
    const parsedWhere = sqlWhereParser.parse(json.where, (operator, operands) => {
      if (['AND', 'OR'].includes(operator)) {
        return `(${operands[0]} ${operator} ${operands[1]})`
      }
      if (operator === 'STARTSWITH') {
        operator = 'STARTS WITH'
      } else if (operator === 'ENDSWITH') {
        operator = 'ENDS WITH'
      }
      const value = operands[1]
      if (isPrimitive(value)) {
        let paramIndex
        if (value instanceof Date) {
          paramIndex = params.push(value.toISOString())
        } else if (typeof value === 'string') {
          paramIndex = params.push(`"${value}"`)
        } else {
          paramIndex = params.push(value)
        }
        return `(${variableName}.${operands[0]} ${operator} $${paramIndex})`
      }
    })
    whereQuery.push(parsedWhere)
  }
  if (whereQuery.length > 1) {
    return whereQuery.join(' ')
  }
  return ''
}

function getOrderCypher (json, variable) {
  if (json.order) {
    let orderBy
    if (typeof json.order === 'string') {
      orderBy = [json.order]
    } else {
      orderBy = json.order
    }
    const cypherOrder = orderBy
      .map(o => (o.startsWith('!') ? `${variable}.${o.slice(1)} DESC` : `${variable}.${o} ASC`))
      .join(', ')
    return `ORDER BY ${cypherOrder}`
  }
  return ''
}

function getPaginationCypher (json, params) {
  const query = []
  if (json.skip > 0) {
    const paramIndex = params.push(json.skip)
    query.push(`SKIP $${paramIndex}`)
  }
  if (json.limit > 0) {
    const paramIndex = params.push(json.limit)
    query.push(`LIMIT $${paramIndex}`)
  }
  return query.join(' ')
}

function getMatchSufix (json, variable, queryEnd) {
  const query = []
  const params = []
  const whereQuery = getWhereCypher(params, json, variable)
  const orderQuery = getOrderCypher(json, variable)
  const paginationQuery = getPaginationCypher(json, params)
  if (whereQuery) {
    query.push(whereQuery)
  }
  query.push(queryEnd)
  if (orderQuery) {
    query.push(orderQuery)
  }
  if (paginationQuery) {
    query.push(paginationQuery)
  }

  return {
    query: query.join(' '),
    params
  }
}

function getWith (variables) {
  return `WITH ${[...variables].join(', ')}`
}

function variablesToCypherWith (variables, json, params) {
  const query = [getWith(variables)]
  const variablesList = [...variables]
  if (json) {
    const order = getOrderCypher(json, variablesList[variablesList.length - 1])
    if (order) {
      query.push(order)
    }
    const pagination = getPaginationCypher(json, params)
    if (pagination) {
      query.push(pagination)
    }
  }
  return query.join(' ')
}

function addPartialQuery (queries, query, variables, json, params) {
  queries.push(`${query} ${variablesToCypherWith(variables, json, params)}`)
}

function validateLabel (label) {
  if (!Array.isArray(label)) {
    label = [label]
  }
  for (const l of label) {
    if (!l) {
      throw new Error('Every object in the json must have a label defined')
    }
    if (!labelRegex.test(l)) {
      throw new Error(`Invalid label: ${l}`)
    }
  }
}

function getCypher (queries, params, graph) {
  return {
    rootKey: 'v1',
    query: queries.join('\n'),
    params,
    graph
  }
}

function queryObjectToCypher (json, getQueryEnd) {
  const { nodes, relationships, root } = getNodesAndRelationships(json)
  if (!relationships.length) {
    const rootMatchSufix = getMatchSufix(root, 'v1', getQueryEnd(new Set(['v1'])))
    return {
      query: `MATCH (v1) ${rootMatchSufix.query}`,
      params: rootMatchSufix.params
    }
  }

  const indexesPerNode = getIndexesPerNode(nodes)
  const statements = []
  const variables = new Set(['v1'])
  const rootMatchSufix = getMatchSufix(root, 'v1', getWith(variables))
  const rootStatement = {
    query: `MATCH (v1) ${rootMatchSufix.query}`,
    params: rootMatchSufix.params
  }
  statements.push(rootStatement)
  for (let rIndex = 0; rIndex < relationships.length; rIndex++) {
    const r = relationships[rIndex]
    const toNode = nodes[r.to]
    const fromIndex = indexesPerNode[r.from]
    const toIndex = indexesPerNode[r.to]
    variables.add(`v${fromIndex}`)
    variables.add(`r${rIndex}`)
    variables.add(`v${toIndex}`)
    const relationshipMatchSufix = getMatchSufix(toNode, `v${toIndex}`, getWith(variables))
    const relationshipStatement = {
      query: `MATCH (v${fromIndex})-[r${rIndex}:${r.name}]->(v${toIndex}) ${relationshipMatchSufix.query}`,
      params: relationshipMatchSufix.params
    }
    if (toNode.where || toNode.order || toNode.skip || toNode.limit) {
      toNode.mandatory = true
    }
    if (!toNode.mandatory) {
      relationshipStatement.query = `OPTIONAL ${relationshipStatement.query}`
    }
    statements.push(relationshipStatement)
  }
  statements.push({ query: getQueryEnd(variables), params: [] })

  return statements.reduce(
    (result, statement) => {
      result.query += `\n${statement.query}`
      result.params.push(...statement.params)
      return result
    },
    { query: '', params: [], rootKey: 'v1' }
  )
}

function getMatches (string, regex) {
  const matches = []
  let m
  do {
    m = regex.exec(string)
    if (m) {
      matches.push(m)
    }
  } while (m)
  return matches.length && matches
}

module.exports = class extends EventEmitter {
  async jsonToCypherWrite (json, options = {}) {
    const graph = getNodesAndRelationships(json)
    const params = [graph.root]
    const indexesPerNode = getIndexesPerNode(graph.nodes)
    const queries = []
    const variables = new Set(['v1'])
    if (graph.root._id) {
      if (options.hooks && options.hooks.beforeUpdate) {
        if (!(await options.hooks.beforeUpdate(graph.root, graph.rootObject))) {
          throw new Error(
            `beforeUpdate hook didn't return truthy value for node ${JSON.stringify(graph.root, null, 2)}`
          )
        }
      }
      this.emit('update', graph.root, graph.rootObject)
      addPartialQuery(
        queries,
        `MATCH (v1:${graph.root._label}) WHERE id(v1) = ${graph.root._id} SET v1 = $1`,
        variables
      )
    } else {
      if (options.hooks && options.hooks.beforeCreate) {
        if (!(await options.hooks.beforeCreate(graph.root, graph.rootObject))) {
          throw new Error(
            `beforeCreate hook didn't return truthy value for node ${JSON.stringify(graph.root, null, 2)}`
          )
        }
      }
      this.emit('create', graph.root, graph.rootObject)
      addPartialQuery(queries, `CREATE (v1:${graph.root._label} $1)`, variables)
    }
    for (let rIndex = 0; rIndex < graph.relationships.length; rIndex++) {
      const r = graph.relationships[rIndex]
      const fromIndex = indexesPerNode[r.from]
      const toIndex = indexesPerNode[r.to]
      params[toIndex - 1] = graph.nodes[r.to]
      variables.add(`v${fromIndex}`)
      variables.add(`r${rIndex}`)
      if (graph.nodes[r.to]._id) {
        if (options.hooks && options.hooks.beforeUpdate) {
          const ok = await options.hooks.beforeUpdate(graph.nodes[r.to], graph.objects[r.to])
          if (!ok) {
            throw new Error(
              `beforeUpdate hook didn't return truthy value for node ${JSON.stringify(graph.nodes[r.to], null, 2)}`
            )
          }
        }
        this.emit('update', graph.nodes[r.to], graph.objects[r.to])
        const paramIndex = params.push(`"${graph.nodes[r.to]._id}"`)
        variables.add(`v${toIndex}`)
        addPartialQuery(
          queries,
          `MATCH (v${fromIndex})-[r${rIndex}]->(v${toIndex}) WHERE id(v${toIndex}) = $${paramIndex} SET v${toIndex} = $${toIndex}`,
          variables
        )
      } else {
        if (options.hooks && options.hooks.beforeCreate) {
          const ok = await options.hooks.beforeCreate(graph.nodes[r.to], graph.objects[r.to])
          if (!ok) {
            throw new Error(
              `beforeCreate hook didn't return truthy value for node ${JSON.stringify(graph.nodes[r.to], null, 2)}`
            )
          }
        }
        this.emit('create', graph.nodes[r.to], graph.objects[r.to])
        if (variables.has(`v${toIndex}`)) {
          addPartialQuery(
            queries,
            `CREATE (v${fromIndex})-[r${rIndex}: ${r.name}${
              r.metadata ? JSON.stringify(r.metadata) : ''
            }]->(v${toIndex})`,
            variables
          )
        } else {
          variables.add(`v${toIndex}`)
          const toLabel = graph.nodes[r.to]._label ? `:${graph.nodes[r.to]._label}` : ''
          addPartialQuery(
            queries,
            `CREATE (v${fromIndex})-[r${rIndex}: ${r.name}${
              r.metadata ? JSON.stringify(r.metadata) : ''
            }]->(v${toIndex}${toLabel} $${toIndex})`,
            variables
          )
        }
      }
    }
    queries.push('RETURN v1,*')
    return getCypher(queries, params, graph)
  }

  jsonToCypherRead (json) {
    return queryObjectToCypher(json, () => 'RETURN *')
  }

  jsonToCypherDelete (json) {
    return queryObjectToCypher(json, variables => {
      const nodes = [...variables].filter(v => v.startsWith('v'))
      return `DETACH DELETE ${nodes.join(',')}`
    })
  }

  jsonToCypherRelationshipDelete (json) {
    return queryObjectToCypher(json, variables => {
      const relationships = [...variables].filter(v => v.startsWith('r'))
      return `DELETE ${relationships.join(',')}`
    })
  }

  getSchema (json) {
    const { nodes, relationships } = getNodesAndRelationships(json)
    const relationshipNames = relationships.filter(r => r.name).map(r => r.name)
    const nodeLabels = [...new Set(Object.values(nodes).map(n => n._label))]
    return {
      relationshipNames,
      nodeLabels
    }
  }

  async parseRows (rows, rootKey, options = {}) {
    const relationships = {}
    const nodes = {}
    const roots = {}
    for (const item of rows) {
      if (item[rootKey]) {
        const rootMatch = getMatches(item[rootKey], nodeRegex)[0]
        const root = {
          _id: rootMatch.groups.id,
          _label: rootMatch.groups.label,
          ...JSON.parse(rootMatch.groups.node)
        }
        roots[root._id] = true
      }
      for (const column of Object.values(item)) {
        if (column && column !== '[]') {
          const matches = getMatches(column, nodeRegex) || getMatches(column, relationshipRegex)
          if (!matches && Array.isArray(column)) {
            for (const i of column) {
              nodes[i.node_id] = { ...nodes[i.node_id], _wkt: i.geom }
            }
          }
          for (let index = 0; index < matches.length; index++) {
            const match = matches[index]
            if (match.groups.node) {
              const node = {
                _id: match.groups.id,
                _label: match.groups.label,
                ...JSON.parse(match.groups.node)
              }
              const object = options.graph && options.graph.objects[node.konekto_id]
              delete node.konekto_id
              if (options.hooks && options.hooks.beforeRead) {
                if (!(await options.hooks.beforeRead(node, object))) {
                  continue
                }
              }
              this.emit('read', node, object)
              nodes[match.groups.id] = { ...nodes[match.groups.id], ...node }
            } else {
              relationships[match.groups.id] = match.groups
            }
          }
        }
      }
    }
    if (!(Object.keys(relationships).length === 0 && relationships.constructor === Object)) {
      for (const rel of Object.values(relationships)) {
        nodes[rel.to][isRelated] = true
        nodes[rel.from] = nodes[rel.from]
        if (rel.metadata && JSON.parse(rel.metadata).isArray) {
          const relKey = nodes[rel.from][rel.label]
          if (relKey) {
            relKey.push(nodes[rel.to])
          } else {
            nodes[rel.from][rel.label] = [nodes[rel.to]]
          }
        } else {
          nodes[rel.from][rel.label] = nodes[rel.to]
        }
      }
      if (Object.values(nodes).length === 1) {
        const node = Object.values(nodes)[0]
        if (node[isRelated]) {
          delete node[isRelated]
        }
        return Object.values(nodes)
      }
      for (const node of Object.values(nodes)) {
        if (node[isRelated]) {
          delete node[isRelated]
        }
        if (!roots[node._id]) {
          delete nodes[node._id]
        }
      }
    }

    return Object.values(nodes)
  }
}