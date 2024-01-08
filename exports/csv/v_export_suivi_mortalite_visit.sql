DROP VIEW IF EXISTS gn_monitoring.v_export_suivi_mortalite_visit;
CREATE OR REPLACE VIEW gn_monitoring.v_export_suivi_mortalite_visit as
select tm.module_code as TypeDonnee,
    tbs.base_site_code as idEolienne,
    tbv.id_base_site,
    tbv.id_base_visit,
    tbv.visit_date_min as dateDebut,
    tbv.visit_date_max as dateFin,
    ref_nomenclatures.get_nomenclature_label(
        (tvc.data->>'id_nomenclature_forme_surface')::integer
    ) as FormeSurface,
    ref_nomenclatures.get_nomenclature_label(
        (tvc.data->>'id_nomenclature_type_surface')::integer
    ) as TypeSurface,
    ref_nomenclatures.get_nomenclature_label((tvc.data->>'id_type_donnee')::integer) as TypeDonnee,
    tvc.data->>'heure_debut_recherche' as DureeRecherche,
    tvc.data->>'longueur_surface' as LongSurface,
    tvc.data->>'surface_recherche' as SurfRecherche,
    tvc.data->>'surface_veg' as SurfaceVeg,
    tvc.data->>'hauteur_moyenne_veg' as HauteurMoyVeg,
    tvc.data->>'pourcentage_foret' as PourForet,
    string_agg(
        distinct concat (UPPER(tr.nom_role), ' ', tr.prenom_role),
        ', '
        order by concat (UPPER(tr.nom_role), ' ', tr.prenom_role)
    ) as observers,
    string_agg(bo.nom_organisme, ','),
    tvc.data->>'observers_txt' as obsExterieurs,
    tbv.id_dataset,
    td.dataset_name
from gn_monitoring.t_base_visits tbv
    join gn_monitoring.t_visit_complements tvc on tvc.id_base_visit = tbv.id_base_visit
    join gn_monitoring.cor_visit_observer cvo on cvo.id_base_visit = tbv.id_base_visit
    join utilisateurs.t_roles tr on tr.id_role = cvo.id_role
    join utilisateurs.bib_organismes bo on tr.id_organisme = bo.id_organisme
    left join gn_meta.t_datasets td on td.id_dataset = tbv.id_dataset
    left join gn_monitoring.t_base_sites tbs on tbs.id_base_site = tbv.id_base_site
    left join gn_monitoring.cor_type_site cts on tbs.id_base_site = cts.id_base_site
    left join gn_monitoring.cor_module_type cmt on cmt.id_type_site = cts.id_type_site
    left join gn_commons.t_modules tm on tm.id_module = cmt.id_module
where tm.module_code::text = 'suivi_mortalite'
group by tbv.id_base_site,
    tm.module_code,
    tbv.id_base_visit,
    td.dataset_name,
    tvc.data,
    tbs.base_site_code