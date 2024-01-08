DROP VIEW IF EXISTS gn_monitoring.v_export_suivi_mortalite;

CREATE OR REPLACE VIEW gn_monitoring.v_export_suivi_mortalite as
SELECT 
tm.module_code,
ref_nomenclatures.get_nomenclature_label((cmt.id_type_site)::integer) AS type_site,
s.base_site_name AS nomEolienne,
s.base_site_code AS idEolienne,
s.geom AS CoordEol,
st_x(st_centroid(s.geom)) AS longitude,
st_y(st_centroid(s.geom)) AS latitude,
s.altitude_min as "altitudeMin",
s.altitude_max as "altitudeMax",
a.jname->>'COM' AS commune,
a.jcode->>'DEP' AS code_departement,
a.jname->>'DEP' as departement,
concat(tr.nom_role, ' ', tr.prenom_role) as numerEol,
org.nom_organisme as organismeNumer,
ref_nomenclatures.get_nomenclature_label((tsc.data->>'id_nomenclature_contexte_saisie')::integer) AS ContSaisie,
ref_nomenclatures.get_nomenclature_label((tsc.data->>'id_nomenclature_type_lisiere')::integer) AS TypeLisiere,
tsc.data->>'modele_eolienne' AS ModEolienne,
tsc.data->>'longueur_pales' AS LongPales,
tsc.data->>'hauteur_eolienne' AS HautEolienne,
tsc.data->>'distance_lisiere' AS DistLisiereArb
from gn_monitoring.t_base_sites s
LEFT JOIN gn_monitoring.t_site_complements tsc ON s.id_base_site = tsc.id_base_site 
JOIN LATERAL ( SELECT d_1.id_base_site ,
        json_object_agg(d_1.type_code, d_1.o_name) AS jname,
        json_object_agg(d_1.type_code, d_1.o_code) AS jcode
       FROM ( SELECT sa.id_base_site ,
                ta.type_code,
                string_agg(DISTINCT a_1.area_name::text, ','::text) AS o_name,
                string_agg(DISTINCT a_1.area_code::text, ','::text) AS o_code
               FROM gn_monitoring.cor_site_area  sa
                 JOIN ref_geo.l_areas a_1 ON sa.id_area = a_1.id_area
                 JOIN ref_geo.bib_areas_types ta ON ta.id_type = a_1.id_type
              WHERE sa.id_base_site  = s.id_base_site
              GROUP BY sa.id_base_site , ta.type_code) d_1
      GROUP BY d_1.id_base_site) a ON true
left join utilisateurs.t_roles tr on tr.id_role = s.id_digitiser 
join utilisateurs.bib_organismes org on
	org.id_organisme = tr.id_organisme
left join gn_monitoring.cor_type_site cts on s.id_base_site = cts.id_base_site 
left join gn_monitoring.cor_module_type cmt on cmt.id_type_site = cts.id_type_site  
left join gn_commons.t_modules tm on tm.id_module = cmt.id_module
WHERE tm.module_code::text = 'suivi_mortalite'
-- visit
tbv.id_base_visit AS id_visit,
tbv.id_dataset AS id_dataset,
td.dataset_name AS jeu_de_données,
tbv.visit_date_min AS date_visite,

string_agg(
        concat(tr.nom_role, ' ', tr.prenom_role)
        , ', '
) AS observateurs,

concat(
	floor(extract('EPOCH' from age(
		concat(tbv.visit_date_max, ' ',tvc."data"->>'heure_fin')::timestamp, 
		concat(tbv.visit_date_min, ' ',tvc."data"->>'heure_debut')::timestamp
	)/3600)
	),
	'h',
	to_char(
        concat(tbv.visit_date_max, ' ',tvc."data"->>'heure_fin')::timestamp 
        - concat(tbv.visit_date_min, ' ',tvc."data"->>'heure_debut')::timestamp, 'MI'
	),
	'm'
)AS temps_releve,

-- observations
obs.uuid_observation as uuid_observation,
split_part(tn4.mnemonique,'-',1) as count_tranche_min,
CASE 
	WHEN split_part(tn4.mnemonique,'-',1) = '>100' THEN 'Indéterminé'
	ELSE split_part(tn4.mnemonique,'-',2)
END AS count_tranche_max,
CASE  
	WHEN toc."data"->>'countExact' IS NOT NULL THEN 'Compté'
	WHEN toc."data"->>'countTranche' IS NOT NULL 
                AND toc."data"->>'countExact' IS NULL THEN 'Estimé'
END AS type_denombrement,
t.lb_nom AS nomScientifiqueRef,
t.nom_complet AS nomVernaculaire,
t.nom_vern AS tax_nom_vern,
t.cd_nom AS CD_nom,
t.regne AS Regne,
t.classe AS Classe,
t.ordre AS Ordre,
t.famille AS Famille,
CASE 
        WHEN t.id_rang IN ('SSES', 'ES') THEN split_part(t.lb_nom, ' ', 1)
        WHEN t.id_rang = 'GN' THEN t.lb_nom
        ELSE NULL
END AS Genre,
ref_nomenclatures.get_nomenclature_label((toc."data"->>'id_nomenclature_behaviour')::integer) AS comportement,
ref_nomenclatures.get_nomenclature_label((toc."data"->>'etat_biologique')::integer) AS etat_biologique,
ref_nomenclatures.get_nomenclature_label((toc."data"->>'id_nomenclature_stade')::integer) AS stade_de_vie,
ref_nomenclatures.get_nomenclature_label((toc."data"->>'sexe')::integer) AS sexe,
obs."comments" AS commentaire_observation,

array_to_string(
	ARRAY( 
		SELECT 
			CASE 
				WHEN entity_id.entity_id = 'no_media' THEN NULL
				ELSE concat('https://geonature.snpn.com/geonature/api/media/attachments/', entity_id.entity_id)
			END
        FROM unnest(
        	ARRAY(
        		SELECT
        			CASE
	        			WHEN tmed3.media_path IS NULL THEN '{no_media}' 
	        			ELSE tmed3.media_path
	        		END
	        )
        ) entity_id(entity_id)
	), ', '
) AS public_media_path_observations

FROM gn_monitoring.t_base_sites s
LEFT JOIN gn_monitoring.t_site_complements tsc ON s.id_base_site = tsc.id_base_site 
JOIN LATERAL ( SELECT d_1.id_base_site ,
        json_object_agg(d_1.type_code, d_1.o_name) AS jname,
        json_object_agg(d_1.type_code, d_1.o_code) AS jcode
       FROM ( SELECT sa.id_base_site ,
                ta.type_code,
                string_agg(DISTINCT a_1.area_name::text, ','::text) AS o_name,
                string_agg(DISTINCT a_1.area_code::text, ','::text) AS o_code
               FROM gn_monitoring.cor_site_area  sa
                 JOIN ref_geo.l_areas a_1 ON sa.id_area = a_1.id_area
                 JOIN ref_geo.bib_areas_types ta ON ta.id_type = a_1.id_type
              WHERE sa.id_base_site  = s.id_base_site
              GROUP BY sa.id_base_site , ta.type_code) d_1
      GROUP BY d_1.id_base_site) a ON true
JOIN gn_monitoring.t_base_visits tbv ON tbv.id_base_site = s.id_base_site 
LEFT JOIN gn_monitoring.t_visit_complements tvc ON tvc.id_base_visit = tbv.id_base_visit
JOIN gn_commons.t_modules m ON m.id_module = tbv.id_module
LEFT JOIN gn_monitoring.cor_visit_observer cvo ON cvo.id_base_visit = tbv.id_base_visit 
LEFT JOIN utilisateurs.t_roles tr ON tr.id_role = cvo.id_role 
JOIN gn_monitoring.t_observations obs ON obs.id_base_visit = tbv.id_base_visit 
LEFT JOIN gn_monitoring.t_observation_complements toc ON toc.id_observation = obs.id_observation 
LEFT JOIN gn_monitoring.cor_site_module csm ON s.id_base_site = csm.id_base_site 
LEFT JOIN gn_commons.t_modules tm ON tm.id_module = csm.id_module
LEFT JOIN taxonomie.taxref t ON t.cd_nom = obs.cd_nom 
LEFT JOIN ref_nomenclatures.t_nomenclatures tn4 ON (toc."data"->>'countTranche')::integer = tn4.id_nomenclature
LEFT JOIN gn_meta.t_datasets td ON td.id_dataset = tbv.id_dataset

LEFT JOIN lateral (
	SELECT 
		array_agg(
			concat(tmed.media_path, ' (titre : ', tmed.title_fr, ')')
		) AS media_path,
		tmed.uuid_attached_row
	FROM gn_commons.t_medias tmed 
	WHERE tmed.uuid_attached_row IS NOT NULL
	GROUP BY tmed.uuid_attached_row) tmed3 ON tmed3.uuid_attached_row = obs.uuid_observation
	
WHERE m.module_code::text = 'amphibians'
GROUP BY 
s.base_site_name,
s.id_base_site,
tbv.id_base_visit,
tbv.visit_date_min,
td.dataset_name,
tvc."data",
toc."data",
tsc.data,
obs.id_observation,
t.lb_nom,
t.nom_vern,
t.cd_nom,
tn4.mnemonique,
tbv.id_dataset,
tm.module_label,
obs."comments",
a.jname->>'COM',
a.jcode->>'COM',
a.jname->>'SEC',
tmed3.media_path
ORDER BY
        s.id_base_site ASC,
        tbv.id_base_visit ASC,
        obs.id_observation ASC;
