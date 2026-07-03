import { useEffect, useState, type FocusEvent } from 'react'
import { ArrowLeft } from 'lucide-react'
import { useI18n } from '../../../../../i18n/provider'
import { useAICCStore } from '../../../hooks/use-aicc-store'
import type { ProviderType, ValidationResult, WizardDraft } from '../../../../../api/aicc_mgr'
import { Stepper } from '../../shared/Stepper'
import { StepChooseType, type SnChooseAvailability } from './StepChooseType'
import { StepConnection } from './StepConnection'
import { StepValidation } from './StepValidation'
import { StepReview } from './StepReview'
import { isConnectionValid } from './connectionValidation'

const INITIAL_DRAFT: WizardDraft = {
  provider_type: null,
  name: '',
  endpoint: '',
  protocol_type: null,
  api_key: '',
  auto_sync_models: true,
}

interface WizardShellProps {
  onBack: () => void
  onCreated: () => void
}

export function WizardShell({ onBack, onCreated }: WizardShellProps) {
  const { t } = useI18n()
  const store = useAICCStore()

  const [step, setStep] = useState(0)
  const [draft, setDraft] = useState<WizardDraft>(INITIAL_DRAFT)
  const [validation, setValidation] = useState<ValidationResult | null>(null)
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)
  const [keyboardInset, setKeyboardInset] = useState(0)
  const [snAvailability, setSnAvailability] = useState<SnChooseAvailability>({
    status: 'checking',
    reason: t('aiCenter.wizard.snCheckingModels', 'Checking SN /models permission.'),
  })

  const steps = [
    t('aiCenter.wizard.step.chooseType', 'Choose Type'),
    t('aiCenter.wizard.step.connection', 'Connection'),
    t('aiCenter.wizard.step.validation', 'Validation'),
    t('aiCenter.wizard.step.review', 'Review'),
  ]

  const updateDraft = (partial: Partial<WizardDraft>) => {
    setCreateError(null)
    setDraft((prev) => ({ ...prev, ...partial }))
  }

  const canNext = () => {
    switch (step) {
      case 0: return draft.provider_type !== null && !(draft.provider_type === 'sn_router' && snAvailability.status === 'unavailable')
      case 1: return isConnectionValid(draft)
      case 2: return validation !== null && !validation.errors.some((e) =>
        !e.includes('balance') // allow balance errors
      ) && validation.endpoint_reachable && validation.auth_valid
      case 3: return true
      default: return false
    }
  }

  const handleNext = async () => {
    if (step === 3) {
      setCreating(true)
      setCreateError(null)
      try {
        await new Promise((r) => setTimeout(r, 300))
        await store.addProvider(draft)
        onCreated()
      } catch (error) {
        const message = error instanceof Error ? error.message : ''
        if (message.includes('provider_validation_required') || message.includes('provider_validation_mismatch')) {
          setCreateError(t('aiCenter.wizard.validationExpired', 'Provider validation expired. Please validate again.'))
          setValidation(null)
          setStep(2)
        } else {
          setCreateError(message || t('aiCenter.wizard.createFailed', 'Could not create provider.'))
        }
      } finally {
        setCreating(false)
      }
      return
    }
    if (step === 1) {
      // Reset validation when moving to step 2
      setValidation(null)
      setCreateError(null)
    }
    setStep((s) => s + 1)
  }

  const handlePrev = () => {
    if (step === 0) {
      onBack()
    } else {
      if (step === 2) setValidation(null)
      setStep((s) => s - 1)
    }
  }

  const handleTypeSelect = (type: ProviderType) => {
    if (type === 'sn_router' && snAvailability.status === 'unavailable') {
      return
    }
    updateDraft({
      provider_type: type,
      name: '',
      endpoint: '',
      protocol_type: null,
      api_key: '',
    })
  }

  const keepFocusedFieldVisible = (event: FocusEvent<HTMLDivElement>) => {
    const target = event.target
    if (!(target instanceof HTMLElement)) return
    if (!target.matches('input, textarea, select')) return
    const scrollFocusedTarget = () => {
      const viewport = window.visualViewport
      const rect = target.getBoundingClientRect()
      const visibleBottom = viewport
        ? viewport.offsetTop + viewport.height - 112
        : window.innerHeight - 112
      if (rect.bottom > visibleBottom) {
        window.scrollBy({ top: rect.bottom - visibleBottom + 24, behavior: 'smooth' })
      }
      target.scrollIntoView({ block: 'center', behavior: 'smooth' })
    }
    window.setTimeout(scrollFocusedTarget, 120)
    window.setTimeout(scrollFocusedTarget, 360)
    window.setTimeout(scrollFocusedTarget, 780)
  }

  useEffect(() => {
    let cancelled = false
    setSnAvailability({
      status: 'checking',
      reason: t('aiCenter.wizard.snCheckingModels', 'Checking SN /models permission.'),
    })
    void store.validateConnection({
      ...INITIAL_DRAFT,
      provider_type: 'sn_router',
    }).then((result) => {
      if (cancelled) return
      setSnAvailability(snAvailabilityFromValidation(result, t))
    }).catch((error) => {
      if (cancelled) return
      setSnAvailability({
        status: 'unknown',
        reason: error instanceof Error
          ? error.message
          : t('aiCenter.wizard.snCheckFailed', 'SN /models availability check failed.'),
      })
    })
    return () => {
      cancelled = true
    }
  }, [store, t])

  useEffect(() => {
    if (draft.provider_type === 'sn_router' && snAvailability.status === 'unavailable') {
      updateDraft({ provider_type: null })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snAvailability.status])

  useEffect(() => {
    const viewport = window.visualViewport
    if (!viewport) return
    const updateKeyboardInset = () => {
      const inset = Math.max(0, window.innerHeight - viewport.height - viewport.offsetTop)
      setKeyboardInset(inset)
    }
    updateKeyboardInset()
    viewport.addEventListener('resize', updateKeyboardInset)
    viewport.addEventListener('scroll', updateKeyboardInset)
    return () => {
      viewport.removeEventListener('resize', updateKeyboardInset)
      viewport.removeEventListener('scroll', updateKeyboardInset)
    }
  }, [])

  return (
    <div className="flex h-full min-h-0 flex-col -mx-4 md:-mx-8 -my-4 md:-my-6">
      {/* Header */}
      <div
        className="flex items-center gap-3 px-4 md:px-6 py-3 shrink-0"
        style={{ borderBottom: '1px solid var(--cp-border)' }}
      >
        <button
          type="button"
          onClick={handlePrev}
          className="p-1 rounded-md hover:opacity-70"
          style={{ color: 'var(--cp-muted)' }}
        >
          <ArrowLeft size={18} />
        </button>
        <span className="text-sm font-medium" style={{ color: 'var(--cp-text)' }}>
          {t('aiCenter.wizard.title', 'Add Provider')}
        </span>
      </div>

      {/* Stepper */}
      <div className="px-4 md:px-6 py-3 shrink-0" style={{ borderBottom: '1px solid var(--cp-border)' }}>
        <Stepper steps={steps} current={step} />
      </div>

      {/* Content */}
      <div
        className="min-h-0 flex-1 overflow-y-auto px-4 py-6 pb-52 [scroll-padding-bottom:14rem] md:px-6 md:pb-6"
        style={keyboardInset > 0 ? {
          paddingBottom: `calc(14rem + ${keyboardInset}px)`,
          scrollPaddingBottom: `calc(14rem + ${keyboardInset}px)`,
        } : undefined}
        onFocusCapture={keepFocusedFieldVisible}
      >
        {step === 0 && (
          <StepChooseType
            selected={draft.provider_type}
            onSelect={handleTypeSelect}
            snAvailability={snAvailability}
          />
        )}
        {step === 1 && (
          <StepConnection draft={draft} onUpdate={updateDraft} />
        )}
        {step === 2 && (
          <StepValidation draft={draft} onResult={setValidation} />
        )}
        {step === 3 && (
          <StepReview
            draft={draft}
            validation={validation}
            onToggleAutoSync={(v) => updateDraft({ auto_sync_models: v })}
          />
        )}
        {createError && (
          <div
            className="mt-4 max-w-lg rounded-lg px-3 py-2 text-sm"
            style={{
              background: 'color-mix(in oklch, var(--cp-danger), transparent 90%)',
              border: '1px solid color-mix(in oklch, var(--cp-danger), transparent 60%)',
              color: 'var(--cp-danger)',
            }}
          >
            {createError}
          </div>
        )}
      </div>

      {/* Footer */}
      <div
        className="sticky bottom-0 z-10 flex shrink-0 items-center justify-between px-4 py-3 md:px-6"
        style={{
          borderTop: '1px solid var(--cp-border)',
          background: 'var(--cp-surface)',
          bottom: keyboardInset > 0 ? `${keyboardInset}px` : 'env(keyboard-inset-height, 0px)',
          paddingBottom: 'calc(0.75rem + env(safe-area-inset-bottom))',
        }}
      >
        <button
          type="button"
          onClick={handlePrev}
          className="min-h-11 rounded-lg px-4 py-2 text-sm"
          style={{ color: 'var(--cp-muted)' }}
        >
          {step === 0 ? t('aiCenter.wizard.back', 'Back') : t('aiCenter.wizard.prev', 'Previous')}
        </button>

        {step === 2 && validation && !validation.auth_valid ? (
          <button
            type="button"
            onClick={() => { setValidation(null); setStep(1) }}
            className="min-h-11 rounded-lg px-4 py-2 text-sm font-medium"
            style={{ background: 'var(--cp-warning)', color: '#fff' }}
          >
            {t('aiCenter.wizard.goBackToFix', 'Go Back to Fix')}
          </button>
        ) : (
          <button
            type="button"
            onClick={handleNext}
            disabled={!canNext() || creating}
            className="min-h-11 rounded-lg px-5 py-2 text-sm font-medium transition-opacity disabled:opacity-40"
            style={{ background: 'var(--cp-accent)', color: '#fff' }}
          >
            {step === 3
              ? creating
                ? t('aiCenter.wizard.creating', 'Creating...')
                : t('aiCenter.wizard.create', 'Create Provider')
              : t('aiCenter.wizard.next', 'Next')}
          </button>
        )}
      </div>
    </div>
  )
}

function validationReason(result: ValidationResult, kind: 'endpoint' | 'auth' | 'models'): string | undefined {
  return result.error_details?.find((item) => item.kind === kind)?.message
}

function snAvailabilityFromValidation(
  result: ValidationResult,
  t: ReturnType<typeof useI18n>['t'],
): SnChooseAvailability {
  if (!result.auth_valid) {
    return {
      status: 'unavailable',
      reason: validationReason(result, 'auth')
        ?? result.errors[0]
        ?? t('aiCenter.wizard.snAuthUnavailable', 'SN /models permission denied. SN relay traffic mode and invite-code activation are required.'),
    }
  }
  if (!result.endpoint_reachable || result.errors.length > 0) {
    return {
      status: 'unknown',
      reason: validationReason(result, 'endpoint')
        ?? validationReason(result, 'models')
        ?? result.errors[0]
        ?? t('aiCenter.wizard.snCheckFailed', 'SN /models availability check failed.'),
    }
  }
  return {
    status: 'available',
    reason: t('aiCenter.wizard.snModelsAvailable', '{{count}} models listed by /models.', {
      count: result.models_discovered.length,
    }),
  }
}
