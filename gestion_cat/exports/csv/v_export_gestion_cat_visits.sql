DROP VIEW IF EXISTS gn_monitoring.v_export_gestion_cat;

CREATE OR REPLACE VIEW gn_monitoring.v_export_gestion_cat as
select
tm.module_label as protocole,
-- sites group
concat(tr_sites_gp.nom_role, ' ', tr_sites_gp.prenom_role) as gestionnaire_principal_sites_gp,
array_to_string(ARRAY( SELECT tr.nom_role::text || ' '::text || tr.prenom_role::text
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'id_gestionnaire_secondaire'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id), ', '::text) AS gestionnaire_secondaire_site_gp,

array_to_string(ARRAY( SELECT bo.nom_organisme
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'id_gestionnaire_secondaire'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id
                     join utilisateurs.bib_organismes bo on tr.id_organisme = bo.id_organisme), ', '::text) AS organisme_gestionnaire_secondaire_site_gp,

array_to_string(ARRAY( SELECT tr.nom_role::text || ' '::text || tr.prenom_role::text
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'id_proprietaire'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id), ', '::text) AS proprietaire_site_gp,

array_to_string(ARRAY( SELECT bo.nom_organisme
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'id_proprietaire'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id
                     join utilisateurs.bib_organismes bo on tr.id_organisme = bo.id_organisme), ', '::text) AS organisme_proprietaire_site_gp,
tsg.data->>'gestionnaire_actif',
tsg.sites_group_name as "nom_unite_gestion",
tsg.sites_group_code as "code_unite_gestion",
tsg.sites_group_description as "description_unite_gestion",
tsg.comments as "remarques_groupes_sites",
tsg.data->>'condition_acces',
tsg.data->>'debut_gestion_cat',
tsg.data->>'fin_gestion_cat',

array_to_string(
	ARRAY( 
		SELECT 
			case 
				when entity_id.entity_id = 'no_media' then null
				else concat('https://geonature.snpn.com/geonature/api/media/attachments/', entity_id.entity_id)
			end
        FROM unnest(
        	ARRAY(
        		select
        			case
	        			when tmed_sites_gp.media_path is null then '{no_media}' 
	        			else tmed_sites_gp.media_path
	        		end
	        )
        ) entity_id(entity_id)
	), ', '
) as public_media_path_sites_group,
-- sites
s.base_site_name AS nom_zone_intervention,
s.base_site_code AS code_zone_intervention,
s.base_site_description AS description_zone_intervention,
s.geom AS wkt_4326,
st_x(st_centroid(s.geom)) AS longitude,
st_y(st_centroid(s.geom)) AS latitude,
s.altitude_min as "altitudeMin",
s.altitude_max as "altitudeMax",
a.jname->>'COM' AS commune,
a.jcode->>'DEP' AS département,
concat(tr2.nom_role, ' ', tr2.prenom_role) as numerisateur_site,

array_to_string(ARRAY( SELECT (tr.nom_role::text || ' '::text) || tr.prenom_role::text
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tsc.data -> 'observers'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id), ', '::text) AS conseillers_cat,
                     
array_to_string(ARRAY( SELECT bo.nom_organisme
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tsc.data -> 'observers'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id
                     JOIN utilisateurs.bib_organismes bo on tr.id_organisme = bo.id_organisme), ', '::text) AS organisme_cat,
s.first_use_date as date_zone_intervention,
array_to_string(
	ARRAY( 
		SELECT 
			case 
				when entity_id.entity_id = 'no_media' then null
				else concat('https://geonature.snpn.com/geonature/api/media/attachments/', entity_id.entity_id)
			end
        FROM unnest(
        	ARRAY(
        		select
        			case
	        			when tmed3.media_path is null then '{no_media}' 
	        			else tmed3.media_path
	        		end
	        )
        ) entity_id(entity_id)
	), ', '
) as public_media_path_sites,

-- visits
concat(tr3.nom_role, ' ', tr3.prenom_role) as numerisateur_visite,
array_to_string(ARRAY( SELECT tr.nom_role::text || ' '::text || tr.prenom_role::text
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'visit_observers'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id), ', '::text) AS conseiller_cat_visite,

array_to_string(ARRAY( SELECT bo.nom_organisme
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'visit_observers'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id
                     join utilisateurs.bib_organismes bo on tr.id_organisme = bo.id_organisme), ', '::text) AS conseiller_cat_organisme,
tbv.visit_date_min AS "date_debut_visite",
tbv.visit_date_max AS "date_fin_visite",
tvc."data"->>'date_sollicitation',
tvc."data"->>'type_sollicitation',
tvc."data"->>'details_pratique_gestion',
tvc."data"->>'ajout_pratique_gestion',
case
	when tvc."data"->>'id_suite_a_donner_cat' = '[]' then null
	when tvc."data"->>'id_suite_a_donner_cat' is null then null
	else (
			array_to_string(ARRAY( SELECT tn.mnemonique::text
	        FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc."data" -> 'id_suite_a_donner_cat'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
	        JOIN ref_nomenclatures.t_nomenclatures tn ON tn.id_nomenclature = entity_id.entity_id
	        WHERE  jsonb_typeof(tvc."data" -> 'id_suite_a_donner_cat') = 'array' 
        ), '; '::text)
       
    )
end AS "id_suite_a_donner_cat",
case
	when tvc."data"->>'id_action_cat' = '[]' then null
	when tvc."data"->>'id_action_cat' is null then null
	else (
			array_to_string(ARRAY( SELECT tn.mnemonique::text
	        FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc."data" -> 'id_action_cat'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
	        JOIN ref_nomenclatures.t_nomenclatures tn ON tn.id_nomenclature = entity_id.entity_id
	        WHERE  jsonb_typeof(tvc."data" -> 'id_action_cat') = 'array' 
        ), '; '::text)
       
    )
end AS "id_action_cat",
case
	when tvc."data"->>'id_nature_travaux' = '[]' then null
	when tvc."data"->>'id_nature_travaux' is null then null
	else (
			array_to_string(ARRAY( SELECT tn.mnemonique::text
	        FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc."data" -> 'id_nature_travaux'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
	        JOIN ref_nomenclatures.t_nomenclatures tn ON tn.id_nomenclature = entity_id.entity_id
	        WHERE  jsonb_typeof(tvc."data" -> 'id_nature_travaux') = 'array' 
        ), '; '::text)
       
    )
end AS "id_nature_travaux",

tvc."data"->>'participants' as "participants",
tvc."data"->>'description_travaux' as "description_travaux",
tvc."data"->>'etat_hydrologique' as "etat_hydrologique",
tvc."data"->>'etat_conservation_fonctionnalites' as "etat_conservation_fonctionnalites",
tvc."data"->>'perturbations_menaces' as "perturbations_menaces",
case
	when tvc."data"->>'id_preconisations' = '[]' then null
	when tvc."data"->>'id_preconisations' is null then null
	else (
			array_to_string(ARRAY( SELECT tn.mnemonique::text
	        FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc."data" -> 'id_preconisations'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
	        JOIN ref_nomenclatures.t_nomenclatures tn ON tn.id_nomenclature = entity_id.entity_id
	        WHERE  jsonb_typeof(tvc."data" -> 'id_preconisations') = 'array' 
        ), '; '::text)
       
    )
end AS "id_preconisations",
tvc."data"->>'details_preconisations' as "details_preconisations",
tvc."data"->>'questions_besoins_gestionnaire' as "questions_besoins_gestionnaire",
tvc."data"->>'comments' as "remarques_visite",
tvc."data"->>'conclusion' as "conclusion_visite",
tbv.id_base_visit AS id_visit,
tbv.id_dataset as id_dataset,
td.dataset_name as jeu_de_donnees,
array_to_string(
	ARRAY( 
		SELECT 
			case 
				when entity_id.entity_id = 'no_media' then null
				else concat('https://geonature.snpn.com/geonature/api/media/attachments/', entity_id.entity_id)
			end
        FROM unnest(
        	ARRAY(
        		select
        			case
	        			when tmed2.media_path is null then '{no_media}' 
	        			else tmed2.media_path
	        		end
	        )
        ) entity_id(entity_id)
	), ', '
) as public_media_path_visits

from gn_monitoring.t_base_sites s
LEFT JOIN gn_monitoring.t_site_complements tsc ON s.id_base_site = tsc.id_base_site
JOIN gn_monitoring.t_sites_groups tsg ON tsg.id_sites_group = tsc.id_sites_group 
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
LEFT JOIN utilisateurs.t_roles tr2 ON tr2.id_role = s.id_inventor
LEFT JOIN utilisateurs.t_roles tr3 ON tr3.id_role = tbv.id_digitiser
LEFT JOIN utilisateurs.t_roles tr_sites_gp ON tr_sites_gp.id_role = tsg.id_inventor
left join gn_monitoring.cor_site_module csm on s.id_base_site = csm.id_base_site 
left join gn_commons.t_modules tm on tm.id_module = csm.id_module
LEFT JOIN ref_nomenclatures.t_nomenclatures tn on (tsc."data"->>'id_pratique_de_gestion')::integer = tn.id_nomenclature 
LEFT JOIN ref_nomenclatures.t_nomenclatures tn2 on (tsc."data"->>'id_nature_travaux')::integer = tn2.id_nomenclature 
LEFT JOIN ref_nomenclatures.t_nomenclatures tn3 on (tsc."data"->>'id_type_visite')::integer = tn3.id_nomenclature 
left join lateral (
	select 
		array_agg(
			concat(tmed.media_path, ' (titre : ', tmed.title_fr, ')')
		) as media_path,
		tmed.uuid_attached_row
	from gn_commons.t_medias tmed 
	where tmed.uuid_attached_row is not null 
	group by tmed.uuid_attached_row) tmed2 on tmed2.uuid_attached_row = tbv.uuid_base_visit

left join lateral (
	select 
		array_agg(
			concat(tmed.media_path, ' (titre : ', tmed.title_fr, ')')
		) as media_path,
		tmed.uuid_attached_row
	from gn_commons.t_medias tmed 
	where tmed.uuid_attached_row is not null 
	group by tmed.uuid_attached_row) tmed3 on tmed3.uuid_attached_row = s.uuid_base_site

left join lateral (
	select 
		array_agg(
			concat(tmed.media_path, ' (titre : ', tmed.title_fr, ')')
		) as media_path,
		tmed.uuid_attached_row
	from gn_commons.t_medias tmed 
	where tmed.uuid_attached_row is not null 
	group by tmed.uuid_attached_row) tmed_sites_gp on tmed_sites_gp.uuid_attached_row = s.uuid_sites_group

left join gn_meta.t_datasets td on td.id_dataset = tbv.id_dataset
WHERE m.module_code::text = 'gestion_cat'
GROUP BY 
s.base_site_name,
s.id_base_site,
tbv.id_base_visit,
tbv.visit_date_min,
td.dataset_name,
tsg."data",
tsc."data",
tvc."data",
tr2.nom_role,
tr2.prenom_role,
tr3.nom_role,
tr3.prenom_role,
tn.label_fr,
tn2.label_fr,
tn3.label_fr,
tn4.label_fr,
tn5.label_fr,
tn6.label_fr,
tn7.label_fr,
tbv.id_dataset,
tm.module_label,
a.jname->>'COM',
a.jcode->>'DEP',
tmed2.media_path,
tmed3.media_path,
tmed_sites_gp.media_path
order by
	tsg.id_sites_group asc,
	s.id_base_site asc,
	tbv.id_base_visit asc;
