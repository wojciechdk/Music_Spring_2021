personfradrag = 46700
skattesats = 0.36

indkomst_per_aar = dict()
folkepension_per_aar = 50000

indkomst_per_aar[64] = 277000
indkomst_per_aar[65] = 297000
indkomst_per_aar[66] = 318000
indkomst_per_aar[67] = 350000


for pensionsalder, indkomst in indkomst_per_aar.items():

    indkomst_efter_skat_per_aar =\
        indkomst - (indkomst - personfradrag) * skattesats

    indkomst_med_folkepension = indkomst + folkepension_per_aar
    indkomst_efter_skat_med_folkepension_per_aar = (
            indkomst_med_folkepension
            - (indkomst_med_folkepension - personfradrag) * skattesats)

    indkomst_efter_skat_per_maaned = indkomst_efter_skat_per_aar / 12
    indkomst_efter_skat_med_folkepension_per_maaned = \
        indkomst_efter_skat_med_folkepension_per_aar / 12

    print(f'### Pensionsalder: {pensionsalder} ###')
    print(f'Månedlig indkomst efter skat:')
    print(f'\tUden folkepension: kr. {indkomst_efter_skat_per_maaned:,.2f}')
    print(f'\tMed folkepension: kr. {indkomst_efter_skat_med_folkepension_per_maaned:,.2f}')
    print(f'År uden folkepension: {max(67-pensionsalder, 0)}')
    print()
