<?php

/*
 * This file is part of the GraphAware Neo4j PHP OGM package.
 *
 * (c) GraphAware Ltd <info@graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\OGM;

use Doctrine\Common\Collections\AbstractLazyCollection;
use Doctrine\Common\Collections\ArrayCollection;
use Doctrine\Common\Collections\Collection;
use GraphAware\Common\Type\Node;
use GraphAware\Common\Type\Relationship;
use GraphAware\Neo4j\Client\Stack;
use GraphAware\Neo4j\OGM\Exception\OGMInvalidArgumentException;
use GraphAware\Neo4j\OGM\Metadata\NodeEntityMetadata;
use GraphAware\Neo4j\OGM\Metadata\RelationshipEntityMetadata;
use GraphAware\Neo4j\OGM\Metadata\RelationshipMetadata;
use GraphAware\Neo4j\OGM\Persister\EntityPersister;
use GraphAware\Neo4j\OGM\Persister\FlushOperationProcessor;
use GraphAware\Neo4j\OGM\Persister\RelationshipEntityPersister;
use GraphAware\Neo4j\OGM\Persister\RelationshipPersister;
use GraphAware\Neo4j\OGM\Proxy\LazyCollection;

/**
 * @author Christophe Willemsen <christophe@graphaware.com>
 * @author Tobias Nyholm <tobias.nyholm@gmail.com>
 */
class UnitOfWork
{
    /**
     * Represents an entity that has been created using the new keyword, but not added to the EnttiyManager and UoW
     */
    const STATE_NEW = 'STATE_NEW';

    /**
     * Represents an entity that is being managed by the EntityManger and tracked by the UoW
     */
    const STATE_MANAGED = 'STATE_MANAGED';

    /**
     * Represents a managed entity that is scheduled for getting deleted on the next flush
     */
    const STATE_DELETED = 'STATE_DELETED';

    /**
     * Represents an entity that was being managed by the entity manager but got detached using the detach method
     */
    const STATE_DETACHED = 'STATE_DETACHED';

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * @var EntityManager
     */
    private $entityManager;

    /**
     * @var \Doctrine\Common\EventManager
     */
    private $eventManager;

    /**
     * This object is used for generating the queries stack to run for creating the objects scheduled for creation
     *
     * @var FlushOperationProcessor
     */
    private $flushOperationProcessor;

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores the node entity persister object against the entity fully classified class name
     * [FQCN] -> EntityPersister object
     *
     * @var array
     */
    private $nodePersisters = [];

    /**
     * This map stores the relationship entity persister object against the entity fully classified class name
     * [FQCN] -> RelationshipEntityPersister object
     *
     * @var array
     */
    private $relationshipEntityPersisters = [];

    /**
     * There's only one relationship persister because relationships don't have properties, therefore, do not require
     * special persisters
     *
     * @var RelationshipPersister
     */
    private $relationshipPersister;

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores entities states in the UoW against hashed object id's
     * [oid] -> Entity State
     *
     * @var array
     */
    private $entityStates = [];

    /**
     * This map stores the relationship entities states against their hashed object id
     * [oid] -> RelationshipEntity State
     *
     * @var array
     */
    private $relationshipEntityStates = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores entities objects against their hashed object id's, it serves as the identity map for the UoW
     * [oid] -> Entity object
     *
     * @var array
     */
    private $nodesMap = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores the node graph id against the hashed object's id
     * [oid] -> gid
     *
     * @var array
     */
    private $nodesGIds = [];

    /**
     * This map stores entities objects against their node graph id's
     * [gid] -> Entity object
     *
     * @var array
     */
    private $nodesByGId = [];

    /**
     * This map stores the relationship entity graph id against the hashed object's id
     * [oid] -> gid
     *
     * @var array
     */
    private $reEntitiesGIds = [];

    /**
     * This map stores relationship entities objects against their node graph id's
     * [gid] -> RelationshipEntity object
     *
     * @var array
     */
    private $reEntitiesByGId = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores the last retrieved object for the node from the database against its graph id
     * [gid] -> Entity object
     *
     * @var array
     */
    private $managedNodesVersion = [];

    /**
     * This map stores a reference to the relationships managed by the UoW against the hashed object id of the first
     * node and the property name of the relationship
     * [aoid][propertyName] -> ['entity' => aoid, 'target' => boid, 'rel' => RelationshipMetaData]
     *
     * @var array
     */
    private $managedRelsRefs = [];

    /**
     * This map stores the last retrieved object for the relationship entity from the database against its graph id
     * [gid] -> RelationshipEntity object
     *
     * Notes: Used by detectRelationshipEntityChanges
     *
     * @var array
     */
    private $managedRelEntitiesVersion = [];

    /**
     * This map stores the "Data values" array for the last retrieved object for the relationship entity from the
     * database against the hashed object id
     * [oid] -> Data Values Array (of the RelationshipEntity object)
     *
     * Notes: Used by computeRelationshipEntityPropertiesChanges
     *
     * @var array
     */
    private $reEntitiesOriginalData = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * I really don't know what this is. It seems to storeoriginal node data, but then it also stores rel entity data
     * And most importantly, it's being used anywhere for retrieval, hence is useless
     *
     * @var array
     */
    // TODO: Remove this attribute with all its references
    private $originalEntityData = [];

    /**
     * This attribute does't seem useful for the most part, it's just getting set whenever a new RelEntity is created
     * Not being check3e at any point of te code
     *
     * @var array
     */
    // TODO: Get rid of this
    private $managedRelationshipEntities = [];

    /**
     * This attribute does't seem useful for the most part, it's just getting set whenever a new RelEntity is created
     * Not being check3e at any point of te code
     *
     * @var array
     */
    // TODO: Get rid of this
    private $managedRelationshipEntitiesMap = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores node objects against their hashed object id's for the nodes that will get created
     * [oid] -> Entity object
     *
     * @var array
     */
    private $nodesScheduledForCreate = [];

    private $nodesScheduledForUpdate = [];

    private $nodesScheduledForDelete = [];

    private $nodesSchduledForDetachDelete = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This list stores the struct [entityA, relationshipMetaData, entityB, propertyName] for relationships that will
     * get created
     * [entityA, relationshipMetaData, entityB, propertyName]
     *
     * @var array
     */
    private $relationshipsScheduledForCreate = [];

    private $relationshipsScheduledForDelete = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * This map stores a tuple of [RelationshipEntity, LinkedNode/PointOfView] objects against their hashed object id
     * [oid] -> [RelationshipEntity, pov]
     *
     * @var array
     */
    private $relEntitiesScheduledForCreate = [];

    private $relEntitiesScheduledForUpdate = [];

    private $relEntitiesScheduledForDelete = [];

    //------------------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    /**
     * UnitOfWork constructor.
     *
     * @param EntityManager $manager
     */
    public function __construct(EntityManager $manager)
    {
        $this->entityManager = $manager;
        $this->eventManager = $manager->getEventManager();
        $this->relationshipPersister = new RelationshipPersister();
        $this->flushOperationProcessor = new FlushOperationProcessor($this->entityManager);
    }

    /**
     * @param $entity
     *
     * @throws \Exception
     */
    public function persist($entity)
    {
        if (!$this->isNodeEntity($entity)) {
            return;
        }
        $visited = [];

        $this->doPersist($entity, $visited);
    }

    /**
     * This method does the actual persistence of objects in the UoW. It does the following:
     * 1- Add object to the hashesMap using its hashed object id
     * 2- Marks object as visited
     * 3- Adds object to the nodes scheduled for creation if it's new
     * 4- Cascade persists simple node relationships
     * 5- Cascade persists node relationship entities
     *
     * @param       $entity :Entity to be persisted in the UoW, can be node or relationship entity
     * @param array $visited
     *
     * @throws \Exception
     */
    public function doPersist($entity, array &$visited)
    {
        // Calculates object hashed id and save the object in the map using the id as index
        $oid                  = spl_object_hash($entity);
        $this->nodesMap[$oid] = $entity;

        // If node has been visited before during this persist routine, do nothing
        if (isset($visited[$oid])) {
            return;
        }

        // Set the object as visited in the current persistence routine
        $visited[$oid] = $entity;
        $entityState = $this->getEntityState($entity, self::STATE_NEW);

        switch ($entityState) {
            case self::STATE_MANAGED:
                //$this->nodesScheduledForUpdate[$oid] = $entity;
                break;
            case self::STATE_NEW:
                $this->nodesScheduledForCreate[$oid] = $entity;
                break;
            case self::STATE_DELETED:
                throw new \LogicException(sprintf('Node has been deleted'));
        }

        $this->cascadePersist($entity, $visited);
        $this->traverseRelationshipEntities($entity, $visited);
    }

    /**
     * Cascade persist in node entity relationships by:
     * 1- Traversing all simple relationship associations
     * 2- Persisting all relationships
     *
     * @param       $entity
     * @param array $visited
     *
     * @throws \Exception
     */
    public function cascadePersist($entity, array &$visited)
    {
        // Retrieve simple relationships (Non-entity relationships) from class meta data
        $classMetadata = $this->entityManager->getClassMetadataFor(get_class($entity));
        $associations = $classMetadata->getSimpleRelationships();

        foreach ($associations as $association) {

            // Get association object
            $value = $association->getValue($entity);
            if ($value instanceof LazyCollection) {

                // If association is a lazy one, add without fetch
                $value = $value->getAddWithoutFetch();
            }
            if (is_array($value) || $value instanceof ArrayCollection || $value instanceof Collection) {

                // If association is a collection but not lazy, persist all relationships
                foreach ($value as $assoc) {
                    $this->persistRelationship($entity, $assoc, $association, $visited);
                }
            } else {

                // If association is not a collection at all, just a node
                $entityB = $association->getValue($entity);
                if (is_object($entityB)) {
                    $this->persistRelationship($entity, $entityB, $association, $visited);
                }
            }
        }
    }

    /**
     * This method persists one relationship between two nodes by:
     * 1- Adding the relationship nodes, metadata, and propery name to the relationshipScheduled for creation
     * 2- Persisting the nodes at the other end of the relationship recursively
     *
     * @param                      $entityA
     * @param                      $entityB
     * @param RelationshipMetadata $relationship
     * @param array                $visited
     *
     * @throws \Exception
     */
    public function persistRelationship($entityA, $entityB, RelationshipMetadata $relationship, array &$visited)
    {
        // Check entityB is a single node or a collection
        // I don't understand why nodeB would be a collection!
        // Seems that this is part of the fix to capture updates that happen on relationships
        if ($entityB instanceof Collection || $entityB instanceof ArrayCollection) {
            foreach ($entityB as $e) {

                // I really don't understand what's happening here
                $aMeta = $this->entityManager->getClassMetadataFor(get_class($entityA));
                $bMeta = $this->entityManager->getClassMetadataFor(get_class($entityB));
                $type = $relationship->isRelationshipEntity() ? $this->entityManager->getRelationshipEntityMetadata($relationship->getRelationshipEntityClass())->getType() : $relationship->getType();
                $hashStr = $aMeta->getIdValue($entityA).$bMeta->getIdValue($entityB).$type.$relationship->getDirection();
                $hash = md5($hashStr);

                // Why are we searching with the hash if we never insert with as index hash?
                if (!array_key_exists($hash, $this->relationshipsScheduledForCreate)) {
                    $this->relationshipsScheduledForCreate[] = [$entityA, $relationship, $e, $relationship->getPropertyName()];
                }

                // Persist recursively for the entityB (the node at the other end of the relationship)
                $this->doPersist($e, $visited);
            }

            return;
        }

        // Persist recursively entityB (the node at the other end of the relationship)
        $this->doPersist($entityB, $visited);

        // Add relationship for scheduled for create relationships
        $this->relationshipsScheduledForCreate[] = [$entityA, $relationship, $entityB, $relationship->getPropertyName()];
    }

    /**
     * Applies changes in the UoW to the graphDB
     *
     * @throws \GraphAware\Neo4j\Client\Exception\Neo4jException
     * @throws \Exception
     */
    public function flush()
    {
        // preFlush
        if ($this->eventManager->hasListeners(Events::PRE_FLUSH)) {
            $this->eventManager->dispatchEvent(Events::PRE_FLUSH, new Event\PreFlushEventArgs($this->entityManager));
        }

        // Detect changes
        $this->detectRelationshipReferenceChanges();
        $this->detectRelationshipEntityChanges();
        $this->computeRelationshipEntityPropertiesChanges();
        $this->detectEntityChanges();
        $statements = [];

        // onFlush
        if ($this->eventManager->hasListeners(Events::ON_FLUSH)) {
            $this->eventManager->dispatchEvent(Events::ON_FLUSH, new Event\OnFlushEventArgs($this->entityManager));
        }

        //--------------------------------------------------------------------------------------------------------------
        // Create nodes scheduled for creation on the graph
        // Capture changes in updated relationship entities since executing persist
        foreach ($this->nodesScheduledForCreate as $nodeToCreate) {

            // To capture changes in relationship entities done after persistence
            $this->traverseRelationshipEntities($nodeToCreate);

            // TODO: Remove these, they are useless
            // Push creation queries to the transaction statements
            $class = get_class($nodeToCreate);
            $persister = $this->getPersister($class);
            $statements[] = $persister->getCreateQuery($nodeToCreate);
        }

        $tx = $this->entityManager->getDatabaseDriver()->transaction();
        $tx->begin();

        // Generate queries stack needed to create all nodes scheduled for creation
        $nodesCreationStack = $this->flushOperationProcessor->processNodesCreationJob($this->nodesScheduledForCreate);
        $results = $tx->runStack($nodesCreationStack);

        foreach ($results as $result) {
            foreach ($result->records() as $record) {
                $oid = $record->get('oid');
                $gid = $record->get('id');

                // Update graph Id in the nodes scheduled for creation list
                $this->hydrateGraphId($oid, $gid);

                // Update references to the created object and update its state to managed
                $this->nodesByGId[$gid]   = $this->nodesScheduledForCreate[$oid];
                $this->nodesGIds[$oid]    = $gid;
                $this->entityStates[$oid] = self::STATE_MANAGED;

                // Save a copy of the current object state (or version) in the managed objects array
                $this->manageEntityReference($oid);
            }
        }

        //--------------------------------------------------------------------------------------------------------------
        // Create relationships scheduled for creation on the graph
        $relStack = $this->entityManager->getDatabaseDriver()->stack('rel_create_schedule');
        foreach ($this->relationshipsScheduledForCreate as $relationship) {
            $statement = $this->relationshipPersister->getRelationshipQuery(
                $this->nodesGIds[spl_object_hash($relationship[0])],
                $relationship[1],
                $this->nodesGIds[spl_object_hash($relationship[2])]
            );
            $relStack->push($statement->text(), $statement->parameters(), $statement->getTag());
        }

        //--------------------------------------------------------------------------------------------------------------
        // Delete relationships scheduled for deletion on the graph
        if (count($this->relationshipsScheduledForDelete) > 0) {
            foreach ($this->relationshipsScheduledForDelete as $relationship) {
                $statement = $this->relationshipPersister->getDeleteRelationshipQuery(
                    $this->nodesGIds[spl_object_hash($relationship[0])],
                    $this->nodesGIds[spl_object_hash($relationship[2])],
                    $relationship[1]
                );
                $relStack->push($statement->text(), $statement->parameters(), $statement->getTag());
            }
        }

        $tx->runStack($relStack);

        //--------------------------------------------------------------------------------------------------------------
        // Create relationship entities scheduled for creation on the graph
        $reStack = Stack::create('rel_entity_create');
        foreach ($this->relEntitiesScheduledForCreate as $oid => $info) {
            $rePersister = $this->getRelationshipEntityPersister(get_class($info[0]));
            $statement = $rePersister->getCreateQuery($info[0], $info[1]);
            $reStack->push($statement->text(), $statement->parameters());
        }

        // Update relationship entities scheduled for update on the graph
        foreach ($this->relEntitiesScheduledForUpdate as $oid => $entity) {
            $rePersister = $this->getRelationshipEntityPersister(get_class($entity));
            $statement = $rePersister->getUpdateQuery($entity);
            $reStack->push($statement->text(), $statement->parameters());
        }

        $results = $tx->runStack($reStack);
        foreach ($results as $result) {
            foreach ($result->records() as $record) {
                $gid = $record->get('id');
                $oid = $record->get('oid');
                $this->hydrateRelationshipEntityId($oid, $gid);
                $this->relationshipEntityStates[$oid] = self::STATE_MANAGED;
            }
        }

        //--------------------------------------------------------------------------------------------------------------
        // Delete relationship entities scheduled for deletion on the graph
        $reDeleteStack = Stack::create('rel_entity_delete');
        foreach ($this->relEntitiesScheduledForDelete as $o) {
            $statement = $this->getRelationshipEntityPersister(get_class($o))->getDeleteQuery($o);
            $reDeleteStack->push($statement->text(), $statement->parameters());
        }

        $results = $tx->runStack($reDeleteStack);
        foreach ($results as $result) {
            foreach ($result->records() as $record) {
                $oid = $record->get('oid');
                $this->relationshipEntityStates[$record->get('oid')] = self::STATE_DELETED;
                $id = $this->reEntitiesGIds[$oid];
                unset($this->reEntitiesGIds[$oid], $this->reEntitiesByGId[$id]);
            }
        }

        //--------------------------------------------------------------------------------------------------------------
        // Update nodes scheduled for update on the graph
        $updateNodeStack = Stack::create('update_nodes');
        foreach ($this->nodesScheduledForUpdate as $entity) {
            $this->traverseRelationshipEntities($entity);
            $statement = $this->getPersister(get_class($entity))->getUpdateQuery($entity);
            $updateNodeStack->push($statement->text(), $statement->parameters());
        }
        $tx->pushStack($updateNodeStack);

        // Delete nodes scheduled for deletion on the graph
        $deleteNodeStack = Stack::create('delete_nodes');
        $possiblyDeleted = [];
        foreach ($this->nodesScheduledForDelete as $entity) {
            if (in_array(spl_object_hash($entity), $this->nodesSchduledForDetachDelete)) {
                $statement = $this->getPersister(get_class($entity))->getDetachDeleteQuery($entity);
            } else {
                $statement = $this->getPersister(get_class($entity))->getDeleteQuery($entity);
            }
            $deleteNodeStack->push($statement->text(), $statement->parameters());
            $possiblyDeleted[] = spl_object_hash($entity);
        }
        $tx->pushStack($deleteNodeStack);

        $tx->commit();

        //--------------------------------------------------------------------------------------------------------------
        // ??
        foreach ($this->relationshipsScheduledForCreate as $rel) {
            $aoid                                   = spl_object_hash($rel[0]);
            $boid                                   = spl_object_hash($rel[2]);
            $field                                  = $rel[3];
            $this->managedRelsRefs[$aoid][$field][] = [
                'entity' => $aoid,
                'target' => $boid,
                'rel' => $rel[1],
            ];
        }

        foreach ($possiblyDeleted as $oid) {
            $this->entityStates[$oid] = self::STATE_DELETED;
        }

        // postFlush
        if ($this->eventManager->hasListeners(Events::POST_FLUSH)) {
            $this->eventManager->dispatchEvent(Events::POST_FLUSH, new Event\PostFlushEventArgs($this->entityManager));
        }

        // Reset all maps references after successful flush
        $this->nodesScheduledForCreate
            = $this->nodesScheduledForUpdate
            = $this->nodesScheduledForDelete
            = $this->nodesSchduledForDetachDelete
            = $this->relationshipsScheduledForCreate
            = $this->relationshipsScheduledForDelete
            = $this->relEntitiesScheduledForUpdate
            = $this->relEntitiesScheduledForCreate
            = $this->relEntitiesScheduledForDelete
            = [];
    }

    /**
     * @throws \Exception
     */
    public function detectEntityChanges()
    {
        $managed = [];
        foreach ($this->entityStates as $oid => $state) {
            if ($state === self::STATE_MANAGED) {
                $managed[] = $oid;
            }
        }

        foreach ($managed as $oid) {
            $id = $this->nodesGIds[$oid];
            $entityA = $this->nodesByGId[$id];
            $visited = [];
            $this->doPersist($entityA, $visited);
            $entityB = $this->managedNodesVersion[$id];
            $this->computeChanges($entityA, $entityB);
        }
    }

    public function addManagedRelationshipReference($entityA, $entityB, $field, RelationshipMetadata $relationship)
    {
        $aoid                                   = spl_object_hash($entityA);
        $boid                                   = spl_object_hash($entityB);
        $this->managedRelsRefs[$aoid][$field][] = [
            'entity' => $aoid,
            'target' => $boid,
            'rel' => $relationship,
        ];
        $this->addManaged($entityA);
        $this->addManaged($entityB);
    }

    public function detectRelationshipEntityChanges()
    {
        $managed = [];
        foreach ($this->relationshipEntityStates as $oid => $state) {
            if ($state === self::STATE_MANAGED) {
                $managed[] = $oid;
            }
        }

        foreach ($managed as $oid) {
            $reA = $this->reEntitiesByGId[$this->reEntitiesGIds[$oid]];
            $reB = $this->managedRelEntitiesVersion[$this->reEntitiesGIds[$oid]];
            $this->computeRelationshipEntityChanges($reA, $reB);
//            $this->checkRelationshipEntityDeletions($reA);
        }
    }

    public function addManagedRelationshipEntity($entity, $pointOfView, $field)
    {
        $id                                                 = $this->entityManager->getRelationshipEntityMetadata(get_class($entity))->getIdValue($entity);
        $oid                                                = spl_object_hash($entity);
        $this->relationshipEntityStates[$oid]               = self::STATE_MANAGED;
        $ref                                                = clone $entity;
        $this->reEntitiesByGId[$id]                         = $entity;
        $this->reEntitiesGIds[$oid]                         = $id;
        $this->managedRelEntitiesVersion[$id]               = $ref;
        $poid                                               = spl_object_hash($pointOfView);
        $this->managedRelationshipEntities[$poid][$field][] = $oid;
        $this->managedRelationshipEntitiesMap[$oid][$poid]  = $field;
        $this->reEntitiesOriginalData[$oid]                         = $this->getOriginalRelationshipEntityData($entity);
    }

    public function getRelationshipEntityById($id)
    {
        if (array_key_exists($id, $this->reEntitiesByGId)) {
            return $this->reEntitiesByGId[$id];
        }

        return null;
    }

    public function detectRelationshipReferenceChanges()
    {
        foreach ($this->managedRelsRefs as $oid => $reference) {
            $entity = $this->nodesByGId[$this->nodesGIds[$oid]];
            foreach ($reference as $field => $info) {
                /** @var RelationshipMetadata $relMeta */
                $relMeta = $info[0]['rel'];
                $value = $relMeta->getValue($entity);
                if ($value instanceof ArrayCollection || $value instanceof AbstractLazyCollection) {
                    $value = $value->toArray();
                }
                if (is_array($value)) {
                    $currentValue = array_map(function ($ref) {
                        return $this->nodesByGId[$this->nodesGIds[$ref['target']]];
                    }, $info);

                    $compare = function ($a, $b) {
                        if ($a === $b) {
                            return 0;
                        }

                        return $a < $b ? -1 : 1;
                    };

                    $added = array_udiff($value, $currentValue, $compare);
                    $removed = array_udiff($currentValue, $value, $compare);

                    foreach ($added as $add) {
                        // Since this is the same property, it should be ok to re-use the first relationship
                        $this->scheduleRelationshipReferenceForCreate($entity, $add, $info[0]['rel']);
                    }
                    foreach ($removed as $remove) {
                        $this->scheduleRelationshipReferenceForDelete($entity, $remove, $info[0]['rel']);
                    }
                } elseif (is_object($value)) {
                    $target = $this->nodesByGId[$this->nodesGIds[$info[0]['target']]];
                    if ($value !== $target) {
                        $this->scheduleRelationshipReferenceForDelete($entity, $target, $info[0]['rel']);
                        $this->scheduleRelationshipReferenceForCreate($entity, $value, $info[0]['rel']);
                    }
                } elseif ($value === null) {
                    foreach ($info as $ref) {
                        $target = $this->nodesByGId[$this->nodesGIds[$ref['target']]];
                        $this->scheduleRelationshipReferenceForDelete($entity, $target, $ref['rel']);
                    }
                }
            }
        }

    }

    public function scheduleRelationshipReferenceForCreate($entity, $target, RelationshipMetadata $relationship)
    {
        $this->relationshipsScheduledForCreate[] = [$entity, $relationship, $target, $relationship->getPropertyName()];
    }

    public function scheduleRelationshipReferenceForDelete($entity, $target, RelationshipMetadata $relationship)
    {
        $this->relationshipsScheduledForDelete[] = [$entity, $relationship, $target, $relationship->getPropertyName()];
    }

    /**
     * This method traverses relationship entities doing the following:
     * 1- Loop over all relationship entities linked to the node
     * 2- Persist all relationship entities
     * 3- Persist the node on the other end of the relationship entity
     *
     * @param       $entity
     * @param array $visited
     *
     * @throws \Exception
     */
    public function traverseRelationshipEntities($entity, array &$visited = [])
    {
        // Loop over all relationship entities tied to the node entity
        $classMetadata = $this->entityManager->getClassMetadataFor(get_class($entity));
        foreach ($classMetadata->getRelationshipEntities() as $relationshipMetadata) {
            $value = $relationshipMetadata->getValue($entity);

            // Skip empty relationship entities
            if (null === $value || ($relationshipMetadata->isCollection() && count($value) === 0)) {
                continue;
            }
            if ($relationshipMetadata->isCollection()) {
                foreach ($value as $v) {

                    // Persist relationship entity
                    $this->persistRelationshipEntity($v, get_class($entity));

                    // Get property that maps to the node at the other ned of the relationship
                    // My guess is that this: persists recursively entityB (the node at the other end of the relationship)
                    $rem = $this->entityManager->getRelationshipEntityMetadata(get_class($v));
                    $toPersistProperty = $rem->getStartNode() === $classMetadata->getClassName() ? $rem->getEndNodeValue($v) : $rem->getStartNodeValue($v);
                    $this->doPersist($toPersistProperty, $visited);
                }
            } else {

                // Persist relationship entity
                $this->persistRelationshipEntity($value, get_class($entity));

                // Get property that maps to the node at the other ned of the relationship
                // My guess is that this: persists recursively entityB (the node at the other end of the relationship)
                $rem = $this->entityManager->getRelationshipEntityMetadata(get_class($value));
                $toPersistProperty = $rem->getStartNode() === $classMetadata->getClassName() ? $rem->getEndNodeValue($value) : $rem->getStartNodeValue($value);
                $this->doPersist($toPersistProperty, $visited);
            }
        }
    }

    /**
     * @param $entity :RelationshipEntity object
     * @param $pov    :Entity representing node linked to RelationshipEntity
     */
    public function persistRelationshipEntity($entity, $pov)
    {
        $oid = spl_object_hash($entity);

        if (!array_key_exists($oid, $this->relationshipEntityStates)) {
            $this->relEntitiesScheduledForCreate[$oid] = [$entity, $pov];
            $this->relationshipEntityStates[$oid] = self::STATE_NEW;
        }
    }

    public function getEntityState($entity, $assumedState = null)
    {
        $oid = spl_object_hash($entity);

        // Check if object already has a state
        if (isset($this->entityStates[$oid])) {
            return $this->entityStates[$oid];
        }

        // Return assumed state as the object's state if it doesn't exist in the UoW's tracked entity states
        if (null !== $assumedState) {
            return $assumedState;
        }

        // Attempt to retrieve object id from its meta data
        $id = $this->entityManager->getClassMetadataFor(get_class($entity))->getIdValue($entity);

        // If object doesn't have an id (yet), then it's new
        if (!$id) {
            return self::STATE_NEW;
        }

        // If the object doesn't have a state in the tracked entity states, but does have an id value, then it's detached
        return self::STATE_DETACHED;
    }

    public function addManaged($entity)
    {
        $oid = spl_object_hash($entity);
        $classMetadata = $this->entityManager->getClassMetadataFor(get_class($entity));
        $id = $classMetadata->getIdValue($entity);
        if (null === $id) {
            throw new \LogicException('Entity marked for managed but could not find identity');
        }
        $this->entityStates[$oid] = self::STATE_MANAGED;
        $this->nodesGIds[$oid]    = $id;
        $this->nodesByGId[$id]    = $entity;
        $this->manageEntityReference($oid);
    }

    /**
     * @param object $entity
     *
     * @return bool
     */
    public function isManaged($entity)
    {
        return isset($this->nodesGIds[spl_object_hash($entity)]);
    }

    public function scheduleDelete($entity, $detachRelationships = false)
    {
        if ($this->isNodeEntity($entity)) {
            $this->nodesScheduledForDelete[] = $entity;
            if ($detachRelationships) {
                $this->nodesSchduledForDetachDelete[] = spl_object_hash($entity);
            }

            return;
        }

        if ($this->isRelationshipEntity($entity)) {
            $this->relEntitiesScheduledForDelete[] = $entity;

            return;
        }

        throw new \RuntimeException(sprintf('Neither Node entity or Relationship entity detected'));
    }

    /**
     * @param int $id
     *
     * @return object|null
     */
    public function getEntityById($id)
    {
        return isset($this->nodesByGId[$id]) ? $this->nodesByGId[$id] : null;
    }

    /**
     * @param $class
     *
     * @return Persister\EntityPersister
     */
    public function getPersister($class)
    {
        if (!array_key_exists($class, $this->nodePersisters)) {
            $classMetadata                = $this->entityManager->getClassMetadataFor($class);
            $this->nodePersisters[$class] = new EntityPersister($this->entityManager, $class, $classMetadata);
        }

        return $this->nodePersisters[$class];
    }

    /**
     * @param $class
     *
     * @return \GraphAware\Neo4j\OGM\Persister\RelationshipEntityPersister
     * @throws \Exception
     */
    public function getRelationshipEntityPersister($class)
    {
        if (!array_key_exists($class, $this->relationshipEntityPersisters)) {
            $classMetadata = $this->entityManager->getRelationshipEntityMetadata($class);
            $this->relationshipEntityPersisters[$class] = new RelationshipEntityPersister($this->entityManager, $class, $classMetadata);
        }

        return $this->relationshipEntityPersisters[$class];
    }

    public function hydrateGraphId($oid, $gid)
    {
        $refl0 = new \ReflectionObject($this->nodesScheduledForCreate[$oid]);
        $p = $refl0->getProperty('id');
        $p->setAccessible(true);
        $p->setValue($this->nodesScheduledForCreate[$oid], $gid);
    }

    public function hydrateRelationshipEntityId($oid, $gid)
    {
        $refl0 = new \ReflectionObject($this->relEntitiesScheduledForCreate[$oid][0]);
        $p = $refl0->getProperty('id');
        $p->setAccessible(true);
        $p->setValue($this->relEntitiesScheduledForCreate[$oid][0], $gid);
        $this->reEntitiesGIds[$oid]            = $gid;
        $this->reEntitiesByGId[$gid]           = $this->relEntitiesScheduledForCreate[$oid][0];
        $this->managedRelEntitiesVersion[$gid] = clone $this->relEntitiesScheduledForCreate[$oid][0];
        $this->reEntitiesOriginalData[$oid]    = $this->getOriginalRelationshipEntityData($this->relEntitiesScheduledForCreate[$oid][0]);
    }

    /**
     * Merges the state of the given detached entity into this UnitOfWork.
     *
     * @param object $entity
     *
     * @return object The managed copy of the entity
     */
    public function merge($entity)
    {
        // TODO write me
        trigger_error('Function not implemented.', E_USER_ERROR);
    }

    /**
     * Detaches an entity from the persistence management. It's persistence will
     * no longer be managed by Doctrine.
     *
     * @param object $entity The entity to detach
     */
    public function detach($entity)
    {
        $visited = [];

        $this->doDetach($entity, $visited);
    }

    /**
     * Refreshes the state of the given entity from the database, overwriting
     * any local, unpersisted changes.
     *
     * @param object $entity The entity to refresh
     */
    public function refresh($entity)
    {
        $visited = [];

        $this->doRefresh($entity, $visited);
    }

    /**
     * Helper method to initialize a lazy loading proxy or persistent collection.
     *
     * @param object $obj
     */
    public function initializeObject($obj)
    {
        // TODO write me
        trigger_error('Function not implemented.', E_USER_ERROR);
    }

    /**
     * @return array
     */
    public function getNodesScheduledForCreate()
    {
        return $this->nodesScheduledForCreate;
    }

    /**
     * @param object $entity
     *
     * @return bool
     */
    public function isScheduledForCreate($entity)
    {
        return isset($this->nodesScheduledForCreate[spl_object_hash($entity)]);
    }

    /**
     * @return array
     */
    public function getNodesScheduledForUpdate()
    {
        return $this->nodesScheduledForUpdate;
    }

    /**
     * @return array
     */
    public function getNodesScheduledForDelete()
    {
        return $this->nodesScheduledForDelete;
    }

    /**
     * @param object $entity
     *
     * @return bool
     */
    public function isScheduledForDelete($entity)
    {
        return isset($this->nodesScheduledForDelete[spl_object_hash($entity)]);
    }

    /**
     * @return array
     */
    public function getRelationshipsScheduledForCreate()
    {
        return $this->relationshipsScheduledForCreate;
    }

    /**
     * @return array
     */
    public function getRelationshipsScheduledForDelete()
    {
        return $this->relationshipsScheduledForDelete;
    }

    /**
     * @return array
     */
    public function getRelEntitiesScheduledForCreate()
    {
        return $this->relEntitiesScheduledForCreate;
    }

    /**
     * @return array
     */
    public function getRelEntitiesScheduledForUpdate()
    {
        return $this->relEntitiesScheduledForUpdate;
    }

    /**
     * @return array
     */
    public function getRelEntitiesScheduledForDelete()
    {
        return $this->relEntitiesScheduledForDelete;
    }

    /**
     * Get the original state of an entity when it was loaded from the database.
     *
     * @param int $gid
     *
     * @return object|null
     */
    public function getOriginalEntityState($gid)
    {
        if (isset($this->managedNodesVersion[$gid])) {
            return $this->managedNodesVersion[$gid];
        }

        return null;
    }

    public function createEntity(Node $node, $className, $id)
    {
        /** todo receive a data of object instead of node object */
        $classMetadata = $this->entityManager->getClassMetadataFor($className);
        $entity = $this->newInstance($classMetadata, $node);
        $oid = spl_object_hash($entity);
        $this->originalEntityData[$oid] = $node->values();
        $classMetadata->setId($entity, $id);
        $this->addManaged($entity);

        return $entity;
    }

    public function createRelationshipEntity(Relationship $relationship, $className, $sourceEntity, $field)
    {
        $classMetadata = $this->entityManager->getClassMetadataFor($className);
        $o = $classMetadata->newInstance();
        $oid = spl_object_hash($o);
        $this->originalEntityData[$oid] = $relationship->values();
        $classMetadata->setId($o, $relationship->identity());
        $this->addManagedRelationshipEntity($o, $sourceEntity, $field);

        return $o;
    }

    /**
     * This method clones the entity version that was last retrieved from the database in a separate map that keeps
     * track of last database object version
     *
     * @param $oid
     */
    private function manageEntityReference($oid)
    {
        // Why not use the hashes? Or nodesScheduledForCreation?
        // Anyways, just get the object
        $gid = $this->nodesGIds[$oid];
        $entity = $this->nodesByGId[$gid];

        // Create a clone of the current object to preserve its state
        $this->managedNodesVersion[$gid] = clone $entity;
    }

    private function computeChanges($entityA, $entityB)
    {
        $classMetadata = $this->entityManager->getClassMetadataFor(get_class($entityA));
        $propertyFields = array_merge($classMetadata->getPropertiesMetadata(), $classMetadata->getLabeledProperties());
        foreach ($propertyFields as $field => $meta) {
            // force proxy to initialize (only needed with proxy manager 1.x
            $reflClass = new \ReflectionClass($classMetadata->getClassName());
            foreach ($reflClass->getMethods() as $method) {
                if ($method->getNumberOfRequiredParameters() === 0 && $method->getName() === 'getId') {
                    $entityA->getId();
                }
            }
            $p1 = $meta->getValue($entityA);
            $p2 = $meta->getValue($entityB);
            if ($p1 !== $p2) {
                $this->nodesScheduledForUpdate[spl_object_hash($entityA)] = $entityA;
            }
        }
    }

    private function computeRelationshipEntityPropertiesChanges()
    {
        foreach ($this->relationshipEntityStates as $oid => $state) {
            if ($state === self::STATE_MANAGED) {
                $e = $this->reEntitiesByGId[$this->reEntitiesGIds[$oid]];
                $cm = $this->entityManager->getClassMetadataFor(get_class($e));
                $newValues = $cm->getPropertyValuesArray($e);
                if (!array_key_exists($oid, $this->reEntitiesOriginalData)) {
                }
                $originalValues = $this->reEntitiesOriginalData[$oid];
                if (count(array_diff($originalValues, $newValues)) > 0) {
                    $this->relEntitiesScheduledForUpdate[$oid] = $e;
                }
            }
        }
    }

    private function computeRelationshipEntityChanges($entityA, $entityB)
    {
        $classMetadata = $this->entityManager->getRelationshipEntityMetadata(get_class($entityA));
        foreach ($classMetadata->getPropertiesMetadata() as $meta) {
            if ($meta->getValue($entityA) !== $meta->getValue($entityB)) {
                $this->relEntitiesScheduledForUpdate[spl_object_hash($entityA)] = $entityA;
            }
        }
    }

    private function getOriginalRelationshipEntityData($entity)
    {
        $classMetadata = $this->entityManager->getClassMetadataFor(get_class($entity));

        return $classMetadata->getPropertyValuesArray($entity);
    }

    private function removeManaged($entity)
    {
        $oid = spl_object_hash($entity);
        unset($this->nodesGIds[$oid]);

        $classMetadata = $this->entityManager->getClassMetadataFor(get_class($entity));
        $id = $classMetadata->getIdValue($entity);
        if (null === $id) {
            throw new \LogicException('Entity marked as not managed but could not find identity');
        }
        unset($this->nodesByGId[$id]);
    }

    /**
     * Executes a detach operation on the given entity.
     *
     * @param object $entity
     * @param array  $visited
     * @param bool   $noCascade if true, don't cascade detach operation
     */
    private function doDetach($entity, array &$visited, $noCascade = false)
    {
        $oid = spl_object_hash($entity);

        if (isset($visited[$oid])) {
            return; // Prevent infinite recursion
        }

        $visited[$oid] = $entity; // mark visited

        switch ($this->getEntityState($entity, self::STATE_DETACHED)) {
            case self::STATE_MANAGED:
                if ($this->isManaged($entity)) {
                    $this->removeManaged($entity);
                }

                unset(
                    $this->nodesScheduledForCreate[$oid],
                    $this->nodesScheduledForUpdate[$oid],
                    $this->nodesScheduledForDelete[$oid],
                    $this->entityStates[$oid]
                );
                break;
            case self::STATE_NEW:
            case self::STATE_DETACHED:
                return;
        }

        $this->entityStates[$oid] = self::STATE_DETACHED;

        if (!$noCascade) {
            $this->cascadeDetach($entity, $visited);
        }
    }

    /**
     * Cascades a detach operation to associated entities.
     *
     * @param object $entity
     * @param array  $visited
     */
    private function cascadeDetach($entity, array &$visited)
    {
        $class = $this->entityManager->getClassMetadata(get_class($entity));

        foreach ($class->getRelationships() as $relationship) {
            $value = $relationship->getValue($entity);

            switch (true) {
                case $value instanceof Collection:
                case is_array($value):
                    foreach ($value as $relatedEntity) {
                        $this->doDetach($relatedEntity, $visited);
                    }
                    break;
                case $value !== null:
                    $this->doDetach($value, $visited);
                    break;
                default:
                    // Do nothing
            }
        }
    }

    /**
     * Executes a refresh operation on an entity.
     *
     * @param object $entity  The entity to refresh
     * @param array  $visited The already visited entities during cascades
     */
    private function doRefresh($entity, array &$visited)
    {
        $oid = spl_object_hash($entity);

        if (isset($visited[$oid])) {
            return; // Prevent infinite recursion
        }

        $visited[$oid] = $entity; // mark visited

        if ($this->getEntityState($entity) !== self::STATE_MANAGED) {
            throw OGMInvalidArgumentException::entityNotManaged($entity);
        }

        $this->getPersister(get_class($entity))->refresh($this->nodesGIds[$oid], $entity);

        $this->cascadeRefresh($entity, $visited);
    }

    /**
     * Cascades a refresh operation to associated entities.
     *
     * @param object $entity
     * @param array  $visited
     */
    private function cascadeRefresh($entity, array &$visited)
    {
        $class = $this->entityManager->getClassMetadata(get_class($entity));

        foreach ($class->getRelationships() as $relationship) {
            $value = $relationship->getValue($entity);

            switch (true) {
                case $value instanceof Collection:
                case is_array($value):
                    foreach ($value as $relatedEntity) {
                        $this->doRefresh($relatedEntity, $visited);
                    }
                    break;
                case $value !== null:
                    $this->doRefresh($value, $visited);
                    break;
                default:
                    // Do nothing
            }
        }
    }

    private function newInstance(NodeEntityMetadata $class, Node $node)
    {
        $proxyFactory = $this->entityManager->getProxyFactory($class);
        /* @todo make possible to instantiate proxy without the node object */
        return $proxyFactory->fromNode($node);
    }

    private function isNodeEntity($entity)
    {
        $meta = $this->entityManager->getClassMetadataFor(get_class($entity));

        return $meta instanceof NodeEntityMetadata;
    }

    private function isRelationshipEntity($entity)
    {
        $meta = $this->entityManager->getClassMetadataFor(get_class($entity));

        return $meta instanceof RelationshipEntityMetadata;
    }
}
