DROP VIEW IF EXISTS gn_monitoring.v_export_suivi_mortalite;

CREATE OR REPLACE VIEW gn_monitoring.v_export_suivi_mortalite as
WITH module AS (
    SELECT
        tm.id_module,
        tm.module_code
    FROM
        gn_commons.t_modules tm
    WHERE
        tm.module_code::text = 'suivi_mortalite'::text
),
sites AS (
    SELECT 
        tm.module_code AS protocole,
        s.id_base_site,
        tsg.sites_group_name AS lbParc,
        tsg.sites_group_code AS idParc,
        tsg.geom AS CoordParc,
        st_x(st_centroid(tsg.geom)) AS longitude,
        st_y(st_centroid(tsg.geom)) AS latitude,
        tsg.altitude_min AS altitudeMinParc,
        tsg.altitude_max AS altitudeMaxParc,
        (rfg_i_com).commune_from_centroid AS communeParc,
        (rfg_i_dep).dep_from_centroid AS departementParc,
        ref_nomenclatures.get_nomenclature_label((cmt.id_type_site)::integer) AS type_site,
        s.base_site_name AS nomEolienne,
        s.base_site_code AS idEolienne,
        s.geom AS CoordEol,
        st_x(st_centroid(s.geom)) AS longitudeEol,
        st_y(st_centroid(s.geom)) AS latitudeEol,
        s.altitude_min AS altitudeMinEol,
        s.altitude_max AS altitudeMaxEol,
        a.jname->>'COM' AS commune,
        a.jcode->>'DEP' AS code_departement,
        a.jname->>'DEP' AS departement,
        concat(tr.nom_role, ' ', tr.prenom_role) AS numerEol,
        org.nom_organisme AS organismeNumerEol,
        ref_nomenclatures.get_nomenclature_label((tsc.data->>'id_nomenclature_contexte_saisie')::integer) AS ContSaisie,
        ref_nomenclatures.get_nomenclature_label((tsc.data->>'id_nomenclature_type_lisiere')::integer) AS TypeLisiere,
        tsc.data->>'modele_eolienne' AS ModEolienne,
        tsc.data->>'longueur_pales' AS LongPales,
        tsc.data->>'hauteur_eolienne' AS HautEolienne,
        tsc.data->>'distance_lisiere' AS DistLisiereArb
    FROM
        gn_monitoring.t_base_sites s
        LEFT JOIN gn_monitoring.t_site_complements tsc ON s.id_base_site = tsc.id_base_site 
        JOIN LATERAL (
            SELECT
                d_1.id_base_site ,
                json_object_agg(d_1.type_code, d_1.o_name) AS jname,
                json_object_agg(d_1.type_code, d_1.o_code) AS jcode
            FROM (
                SELECT
                    sa.id_base_site ,
                    ta.type_code,
                    string_agg(DISTINCT a_1.area_name::text, ','::text) AS o_name,
                    string_agg(DISTINCT a_1.area_code::text, ','::text) AS o_code
                FROM
                    gn_monitoring.cor_site_area sa
                    JOIN ref_geo.l_areas a_1 ON sa.id_area = a_1.id_area
                    JOIN ref_geo.bib_areas_types ta ON ta.id_type = a_1.id_type
                WHERE
                    sa.id_base_site  = s.id_base_site
                GROUP BY
                    sa.id_base_site , ta.type_code
            ) d_1
            GROUP BY
                d_1.id_base_site
        ) a ON true
        LEFT JOIN utilisateurs.t_roles tr ON tr.id_role = s.id_digitiser 
        JOIN utilisateurs.bib_organismes org ON org.id_organisme = tr.id_organisme
        LEFT JOIN gn_monitoring.cor_type_site cts ON s.id_base_site = cts.id_base_site 
        LEFT JOIN gn_monitoring.t_sites_groups tsg ON tsg.id_sites_group = tsc.id_sites_group 
        LEFT JOIN LATERAL (
            SELECT
                area_name AS commune_from_centroid
            FROM 
                ref_geo.fct_get_area_intersection(st_centroid(tsg.geom)) rfg
                JOIN ref_geo.bib_areas_types bat ON rfg.id_type = bat.id_type AND bat.type_code = 'COM'
        ) rfg_i_com ON true
        LEFT JOIN LATERAL (
            SELECT
                area_name AS dep_from_centroid
            FROM 
                ref_geo.fct_get_area_intersection(st_centroid(tsg.geom)) rfg
                JOIN ref_geo.bib_areas_types bat ON rfg.id_type = bat.id_type AND bat.type_code = 'DEP'
        ) rfg_i_dep ON true     
        LEFT JOIN gn_monitoring.cor_module_type cmt ON cmt.id_type_site = cts.id_type_site  
        LEFT JOIN module tm ON tm.id_module = cmt.id_module
),
visites AS (
    SELECT  
    tbv.id_base_site,
        tbv.id_base_visit,
        tbv.visit_date_min AS dateDebut,
        tbv.visit_date_max AS dateFin,
        ref_nomenclatures.get_nomenclature_label(
            (tvc.data->>'id_nomenclature_forme_surface')::integer
        ) AS FormeSurface,
        ref_nomenclatures.get_nomenclature_label(
            (tvc.data->>'id_nomenclature_type_surface')::integer
        ) AS TypeSurface,
        ref_nomenclatures.get_nomenclature_label((tvc.data->>'id_type_donnee')::integer) AS TypeDonnee,
        tvc.data->>'heure_debut_recherche' AS DureeRecherche,
        tvc.data->>'longueur_surface' AS LongSurface,
        tvc.data->>'surface_recherche' AS SurfRecherche,
        tvc.data->>'surface_veg' AS SurfaceVeg,
        tvc.data->>'hauteur_moyenne_veg' AS HauteurMoyVeg,
        tvc.data->>'pourcentage_foret' AS PourForet,
        string_agg(
            DISTINCT CONCAT(UPPER(tr.nom_role), ' ', tr.prenom_role),
            ', ' ORDER BY CONCAT(UPPER(tr.nom_role), ' ', tr.prenom_role)
        ) AS observers_visit,
        string_agg(bo.nom_organisme, ',') AS organizations_visit,
        tvc.data->>'observers_txt' AS obsExterieurs_visit,
        tbv.id_dataset,
        td.dataset_name
    FROM
        gn_monitoring.t_base_visits tbv
        JOIN gn_monitoring.t_visit_complements tvc ON tvc.id_base_visit = tbv.id_base_visit
        JOIN gn_monitoring.cor_visit_observer cvo ON cvo.id_base_visit = tbv.id_base_visit
        JOIN utilisateurs.t_roles tr ON tr.id_role = cvo.id_role
        JOIN utilisateurs.bib_organismes bo ON tr.id_organisme = bo.id_organisme
        LEFT JOIN gn_meta.t_datasets td ON td.id_dataset = tbv.id_dataset
        JOIN sites tbs ON tbs.id_base_site = tbv.id_base_site
    GROUP BY
        tbv.id_base_visit,
        td.dataset_name,
        tvc.data
),
observations AS (
    SELECT 
    	tobs.id_base_visit,
        tobs.id_observation,
        tobs.cd_nom,
        ref_nomenclatures.get_nomenclature_label(
            (toc.data->>'id_nomenclature_type_colision')::integer
        ) AS TypeCollision,
        ref_nomenclatures.get_nomenclature_label(
            (toc.data->>'id_nomenclature_etat_cadavre')::integer
        ) AS EtatCadavre,
        ref_nomenclatures.get_nomenclature_label(
            (toc.data->>'id_nomenclature_presence_foret')::integer
        ) AS PresForet,
        ref_nomenclatures.get_nomenclature_label((toc.data->>'id_nomenclature_sex')::integer) AS Sexe,
        ref_nomenclatures.get_nomenclature_label(
            (toc.data->>'id_nomenclature_bio_status')::integer
        ) AS StatutBiologique,
        ref_nomenclatures.get_nomenclature_label(
            (toc.data->>'id_nomenclature_life_stage')::integer
        ) AS Age,
        toc.data->>'ditance_eol' AS DistEolienne,
        toc.data->>'hauteur_veg' AS HauteurVeg,
        toc.data->>'count_min' AS CountMin,
        toc.data->>'count_max' AS CountMax,
        toc.data->>'heure_obs' AS HeureObs,
        string_agg(
            DISTINCT CONCAT(UPPER(tr.nom_role), ' ', tr.prenom_role),
            ', ' ORDER BY CONCAT(UPPER(tr.nom_role), ' ', tr.prenom_role)
        ) AS observersObs,
        string_agg(bo.nom_organisme, ',') AS organizationsObs,
        CASE
            WHEN toc.data->>'presence_cadavre' = 'Absence de cadavre durant la prospection' THEN NULL
            ELSE (toc.data->>'presence_cadavre')
        END AS "PresCadavre",
        tobs.comments AS commentaireObs,
        array_to_string(
            array(
                SELECT 
                    CASE 
                        WHEN entity_id.entity_id = 'no_media' THEN NULL
                        ELSE CONCAT('https://geonature.test01.natural-solutions.eu/geonature/api/media/attachments/', entity_id.entity_id)
                    END
                FROM unnest(array(SELECT CASE WHEN tmed3.media_path IS NULL THEN '{no_media}' ELSE tmed3.media_path END)) entity_id(entity_id)
            ),
            ', '
        ) AS public_media_path_observations
    FROM 
        gn_monitoring.t_observations tobs
        JOIN visites tbv ON tbv.id_base_visit = tobs.id_base_visit
        JOIN gn_monitoring.t_observation_complements toc ON toc.id_observation = tobs.id_observation
        LEFT JOIN utilisateurs.t_roles tr ON tr.id_role = tobs.id_digitiser
        JOIN utilisateurs.bib_organismes bo ON tr.id_organisme = bo.id_organisme
        LEFT JOIN lateral (
            SELECT 
                array_agg(
                    CONCAT(tmed.media_path, ' (titre : ', tmed.title_fr, ')')
                ) AS media_path,
                tmed.uuid_attached_row
            FROM
                gn_commons.t_medias tmed
            WHERE
                tmed.uuid_attached_row IS NOT NULL
            GROUP BY
                tmed.uuid_attached_row
        ) tmed3 ON tmed3.uuid_attached_row = tobs.uuid_observation
    GROUP BY 
        tobs.id_observation,
        tbv.id_base_visit,
        toc.data,
        tmed3.media_path
)
SELECT 
    observations.id_observation,
    MAX(observations.cd_nom) AS cd_nom,
    MAX(observations.TypeCollision) AS TypeCollision,
    MAX(observations.HauteurVeg) AS HauteurVeg,
    MAX(observations.commentaireObs) AS commentaireObs,
    MAX(observations.public_media_path_observations) AS public_media_path_observations
--     Colonnes à choisir pour les sites et groupes de sites
	-- tbv.*,
	-- tbs.*
FROM 
    observations
INNER JOIN 
    visites tbv ON tbv.id_base_visit = observations.id_base_visit
inner join 
	sites tbs ON tbs.id_base_site = tbv.id_base_site
GROUP BY 
    observations.id_observation;