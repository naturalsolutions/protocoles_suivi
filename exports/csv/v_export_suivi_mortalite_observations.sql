DROP VIEW IF EXISTS gn_monitoring.v_export_suivi_mortalite_observations;
CREATE OR REPLACE VIEW gn_monitoring.v_export_suivi_mortalite_observations as
select tm.module_code as TypeDonnee,
    tbs.base_site_code as idEolienne,
    tbs.id_base_site,
    tbv.id_base_visit,
    tobs.id_observation,
    tobs.cd_nom,
    ref_nomenclatures.get_nomenclature_label(
        (toc.data->>'id_nomenclature_type_colision')::integer
    ) as TypeCollision,
    ref_nomenclatures.get_nomenclature_label(
        (toc.data->>'id_nomenclature_etat_cadavre')::integer
    ) as EtatCadavre,
    ref_nomenclatures.get_nomenclature_label(
        (toc.data->>'id_nomenclature_presence_foret')::integer
    ) as PresForet,
    ref_nomenclatures.get_nomenclature_label((toc.data->>'id_nomenclature_sex')::integer) as Sexe,
    ref_nomenclatures.get_nomenclature_label(
        (toc.data->>'id_nomenclature_bio_status')::integer
    ) as StatutBiologique,
    ref_nomenclatures.get_nomenclature_label(
        (toc.data->>'id_nomenclature_life_stage')::integer
    ) as Age,
    toc.data->>'ditance_eol' as DistEolienne,
    toc.data->>'hauteur_veg' as HauteurVeg,
    toc.data->>'count_min' as CountMin,
    toc.data->>'count_max' as CountMax,
    toc.data->>'heure_obs' as HeureObs,
    string_agg(
        distinct concat (UPPER(tr.nom_role), ' ', tr.prenom_role),
        ', '
        order by concat (UPPER(tr.nom_role), ' ', tr.prenom_role)
    ) as observers,
    string_agg(bo.nom_organisme, ','),
    case
        when toc.data->>'presence_cadavre' = 'Absence de cadavre durant la prospection' then null
        else (toc.data->>'presence_cadavre')
    end as "PresCadavre",
    tobs.comments as commentaireObs,
	array_to_string(
	array(
	select 
			case 
				when entity_id.entity_id = 'no_media' then null
			else concat('https://geonature.snpn.com/geonature/api/media/attachments/',
			entity_id.entity_id)
		end
	from
		unnest(
        	array(
		select
			case
				when tmed3.media_path is null then '{no_media}'
				else tmed3.media_path
			end
	        )
        ) entity_id(entity_id)
	),
	', '
) as public_media_path_observations
from gn_monitoring.t_observations tobs
    join gn_monitoring.t_base_visits tbv on tbv.id_base_visit = tobs.id_base_visit
    join gn_monitoring.t_observation_complements toc on toc.id_observation = tobs.id_observation
    left join utilisateurs.t_roles tr on tr.id_role = tobs.id_digitiser
    join utilisateurs.bib_organismes bo on tr.id_organisme = bo.id_organisme
    left join gn_monitoring.t_base_sites tbs on tbs.id_base_site = tbv.id_base_site
    left join gn_monitoring.cor_type_site cts on tbs.id_base_site = cts.id_base_site
    left join gn_monitoring.cor_module_type cmt on cmt.id_type_site = cts.id_type_site
    left join gn_commons.t_modules tm on tm.id_module = cmt.id_module
    left join lateral (
	select 
		array_agg(
			concat(tmed.media_path,
		' (titre : ',
		tmed.title_fr,
		')')
		) as media_path,
		tmed.uuid_attached_row
	from
		gn_commons.t_medias tmed
	where
		tmed.uuid_attached_row is not null
	group by
		tmed.uuid_attached_row) tmed3 on
	tmed3.uuid_attached_row = tobs.uuid_observation
where tm.module_code::text = 'suivi_mortalite'
group by tobs.id_observation,
    tm.module_code,
    tbs.id_base_site,
    tbv.id_base_visit,
    toc.data,
    tbs.base_site_code,
    tmed3.media_path