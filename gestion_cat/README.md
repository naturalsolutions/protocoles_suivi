# Protocole Gestion CAT

## Installation du protocole

```sh
cd <path-to-geonature>
source backend/venv/bin/activate
geonature monitorings install gestion_cat
```

## Configuration du protocole

Après avoir installé le protocole il faut avoir redéfini les ids suivants dans le fichier `module.json` :

- "ids_diagnostic_suivi" : ['id_diagnostic, 'id_suivi']
- "id_diagnostic": ['id_diagnostic']
- "id_suivi": ['id_suivi']
- "id_intervention": ['id_intervention']

Les ids peuvent être trouvés en réalisant la commande sql suivante : 

```sql
select tn.id_nomenclature, tn.cd_nomenclature
from ref_nomenclatures.t_nomenclatures tn 
join ref_nomenclatures.bib_nomenclatures_types bn 
on tn.id_type = bn.id_type 
where bn.mnemonique = 'TYPE_VISITE'
```