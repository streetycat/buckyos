import { Cloud, Cpu, Globe, Network, Zap, Server } from 'lucide-react'
import { useI18n } from '../../../../../i18n/provider'
import type { KnownProviderProfile, ProviderType } from '../../../../../api/aicc_mgr'

const PROVIDER_ICONS: Record<ProviderType, typeof Network> = {
  sn_router: Network,
  openai: Zap,
  anthropic: Cpu,
  google: Globe,
  openrouter: Cloud,
  minimax: Cpu,
  fal: Cloud,
  custom: Server,
}

interface StepChooseTypeProps {
  selected: ProviderType | null
  onSelect: (type: ProviderType) => void
  hasManagedSnProvider: boolean
  profiles: KnownProviderProfile[]
}

export function StepChooseType({ selected, onSelect, hasManagedSnProvider, profiles }: StepChooseTypeProps) {
  const { t } = useI18n()

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
      {profiles.map((profile) => {
        const active = selected === profile.provider_type
        const isSnRouter = profile.provider_type === 'sn_router'
        const Icon = PROVIDER_ICONS[profile.provider_type]
        return (
          <button
            key={profile.provider_profile_id}
            type="button"
            onClick={() => onSelect(profile.provider_type)}
            disabled={isSnRouter && !hasManagedSnProvider}
            className="flex flex-col gap-2 p-4 rounded-xl text-left transition-all disabled:cursor-not-allowed"
            style={{
              background: active ? 'color-mix(in oklch, var(--cp-accent), transparent 90%)' : 'var(--cp-surface)',
              border: active ? '2px solid var(--cp-accent)' : '1px solid var(--cp-border)',
              opacity: isSnRouter && !hasManagedSnProvider ? 0.72 : 1,
            }}
          >
            <div className="flex items-center gap-2">
              <Icon size={20} style={{ color: active ? 'var(--cp-accent)' : 'var(--cp-muted)' }} />
              <span className="text-sm font-medium" style={{ color: 'var(--cp-text)' }}>
                {profile.display_name}
              </span>
              {isSnRouter && (
                <span
                  className="text-[10px] px-1.5 py-0.5 rounded font-medium"
                  style={{ background: 'var(--cp-accent)', color: '#fff' }}
                >
                  {isSnRouter
                    ? hasManagedSnProvider
                      ? t('aiCenter.wizard.systemManaged', 'System managed')
                      : t('aiCenter.wizard.activationRequired', 'Activation required')
                    : ''}
                </span>
              )}
            </div>
            <span className="text-xs" style={{ color: 'var(--cp-muted)' }}>
              {isSnRouter
                ? hasManagedSnProvider
                  ? t('aiCenter.wizard.snAlreadyManaged', 'Configured automatically from the current Zone SN settings.')
                  : t('aiCenter.wizard.snActivateFirst', 'Activate SN during BuckyOS setup to enable this system provider.')
                : `${profile.protocol_adapter_id} · ${profile.metadata_drivers.join(', ') || t('aiCenter.wizard.conservativeModels', 'Conservative model discovery')}`}
            </span>
          </button>
        )
      })}
    </div>
  )
}
