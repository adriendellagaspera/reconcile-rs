# Présentation et expériences

## Introduction

« Chaque case est une réplique réelle. Elle possède les données, son horloge et son runtime.
On peut écrire localement, couper ses communications, puis laisser le protocole réparer les
divergences. RSOS est le backend ordonné embarqué dans chacune des instances. »

Charger 128 clés depuis A. Attendre l'accord des entrées datées, puis proposer :
« Choisis une réplique, une coupure et une plage à modifier. Qu'est-ce que tu t'attends à voir ? »
La présentation avance au rythme des expériences, sans étapes automatiques.

## Manipulations

| Expérience | Actions | Ce qu'on observe / discute |
|---|---|---|
| Coupure simple | Déconnecter B ; écrire `[32, 64)` sur A ; reconnecter B | La donnée locale reste disponible ; B rejoint les autres après réparation |
| Écriture isolée | Déconnecter C ; écrire `[64, 96)` sur C | C peut écrire sans coordination ; l'accusé confirme la mutation locale |
| Conflit | Pendant une coupure, écrire deux valeurs sur le même intervalle depuis deux répliques | Les valeurs divergent ; la reconnexion retient le plus grand stamp, pas la valeur « correcte » pour le métier |
| Suppression | Isoler une réplique ; supprimer une plage depuis une autre | Tombstones visibles, valeurs supprimées localement ; propagation à la reconnexion |
| Réinsertion | Réécrire une clé après réception de sa suppression | Un nouveau stamp peut dépasser le tombstone |
| Chemin indirect | Couper A–C en laissant A–B et B–C ouverts ; écrire sur A | Une coupure de lien n'isole pas forcément deux répliques si un autre chemin existe |
| Partition complète | Déconnecter deux répliques ; modifier des plages distinctes et communes | Plages distinctes préservées ; conflits résolus clé par clé |
| Flux continu | Activer Live writes ; modifier les communications ; arrêter le flux | Réception pendant l'activité, puis observation plus simple de la convergence une fois les écritures arrêtées |

Les intervalles sont semi-ouverts : `[32, 64)` contient les clés 32 à 63. `[40, 41)` cible une seule clé.
Les tables de réplique montrent les six premières entrées de la sélection ; choisir une plage
plus petite pour inspecter une clé précise. Cliquer une tuile sélectionne sa plage de 32 clés.

## Passer à l'architecture

La page séparée suit les responsabilités : cluster → runtime → backend RSOS → protocole RBSR →
réseau → suppression et persistance. Utiliser les ancres pour répondre à une question plutôt que
parcourir tous les schémas d'un bloc.

- Cluster : disponibilité locale, réplication complète et absence de maître fixe.
- Runtime : horloge, merge, tâches, émission et réparation ; adaptation Tokio / worker.
- RSOS : arbres persistants, racines immuables, partage structurel et agrégats de plages.
- RBSR : comparer, affiner ou énumérer ; les plages de la vue sont des requêtes d'inspection fixes.
- Réseau : authentification, admission, anti-replay, framing ; transport en mémoire dans la démo.
- Suppression / durabilité : tombstones et stabilité causale ; persistance optionnelle hors démo.

## Questions à ouvrir

« Quand LWW serait-il inadapté ? » « Que faudrait-il pour une attribution exclusive de tâches ? »
« Quel coût aurait une longue partition avec beaucoup d'écritures ? » « Comment distinguer
réplication, durabilité et historique ? » « Quels benchmarks faudrait-il pour mesurer le gain des
résumés de plages ? »

Ne pas interpréter les compteurs de datagrammes comme des acquittements. L'accord affiché compare
les entrées datées exactes ; l'égalité d'une empreinte abrégée seule ne le prouve pas. Les schémas
sont conceptuels, pas une trace interne de cette exécution. La démo n'offre ni consensus ni exclusion
mutuelle et ne mesure pas les performances de partage structurel ou de persistance.
